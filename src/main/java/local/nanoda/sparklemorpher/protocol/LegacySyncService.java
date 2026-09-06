package local.nanoda.sparklemorpher.protocol;

import com.micaftic.morpher.core.security.YSMByteBuf;
import com.micaftic.morpher.core.security.YsmCrypt;
import io.netty.buffer.Unpooled;
import local.nanoda.sparklemorpher.storage.ModelStore;
import org.bukkit.entity.Player;

import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.Random;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Server half of Sparkle Morpher's unchanged legacy model-download protocol.
 * The Fabric client intentionally sees the same encrypted payloads as it would
 * from a Fabric server; only packet transport is provided by Bukkit.
 */
public final class LegacySyncService {
    private static final int CHUNK_BYTES = 30_720;
    private static final long SESSION_TIMEOUT_MILLIS = 5 * 60 * 1000L;
    /**
     * The stock client indexes its local server cache using this key. It must
     * remain stable across reconnects, otherwise every login looks like a new
     * cache directory and forces a full model redownload.
     */
    private static final byte[] CACHE_CLIENT_KEY = createStableClientKey();

    private final ModelStore store;
    private final BiConsumer<Player, byte[]> sender;
    private final Map<UUID, Session> sessions = new ConcurrentHashMap<>();
    private final SecureRandom random = new SecureRandom();

    public LegacySyncService(ModelStore store, BiConsumer<Player, byte[]> sender) {
        this.store = store;
        this.sender = sender;
    }

    public void start(Player player) {
        if (player == null || !player.isOnline()) return;
        Session session = new Session(CACHE_CLIENT_KEY.clone());
        sessions.put(player.getUniqueId(), session);
        try {
            sendHandshake(player, session);
        } catch (Exception error) {
            sessions.remove(player.getUniqueId(), session);
        }
    }

    public void accept(Player player, byte[] encryptedPayload) {
        if (player == null || encryptedPayload == null || encryptedPayload.length == 0) return;
        Session session = sessions.get(player.getUniqueId());
        if (session == null || session.expired()) {
            sessions.remove(player.getUniqueId(), session);
            return;
        }
        session.touch();
        try {
            if (session.step == 1) {
                handlePong(player, session, encryptedPayload);
            } else if (session.step == 2) {
                handleRequests(player, session, encryptedPayload);
            }
        } catch (Exception error) {
            sessions.remove(player.getUniqueId(), session);
        }
    }

    public void remove(Player player) {
        if (player != null) sessions.remove(player.getUniqueId());
    }

    private void sendHandshake(Player player, Session session) throws Exception {
        try (YSMByteBuf data = new YSMByteBuf(Unpooled.buffer())) {
            writeGarbage(data);
            data.writeByte((byte) 1);
            YsmCrypt.EncryptedPacket packet = YsmCrypt.encrypt(data.toArray(), YsmCrypt.publicKey, true);
            session.key1 = packet.nextKey();
            send(player, packet.data());
        }
    }

    private void handlePong(Player player, Session session, byte[] encryptedPayload) throws Exception {
        byte[] decrypted = YsmCrypt.decrypt(encryptedPayload, session.key1);
        if (decrypted == null || decrypted.length < 56) throw new IllegalArgumentException("Invalid Sparkle handshake response");
        session.manifestKey = Arrays.copyOfRange(decrypted, decrypted.length - 56, decrypted.length);
        try (YSMByteBuf payload = new YSMByteBuf(Unpooled.wrappedBuffer(decrypted, 0, decrypted.length - 56))) {
            payload.skipGarbageHeader();
            if (payload.readByte() != 2) throw new IllegalArgumentException("Unexpected Sparkle handshake step");
        }
        session.step = 2;
        sendManifest(player, session);
    }

    private void sendManifest(Player player, Session session) throws Exception {
        List<ModelStore.CompiledModel> models = store.compiledModels();
        try (YSMByteBuf data = new YSMByteBuf(Unpooled.buffer())) {
            writeGarbage(data);
            data.writeVarInt(3);
            data.writeVarLong(0L);
            data.getRawBuf().writeBytes(store.serverKey());
            data.getRawBuf().writeBytes(session.clientKey);
            data.writeVarInt(models.size());
            for (ModelStore.CompiledModel model : models) {
                data.writeVarLong(model.hash1());
                data.writeVarLong(model.hash2());
                data.writeString(model.modelId());
                data.writeVarInt(model.auth() ? 1 : 0);
                data.writeVarInt(0);
                data.writeVarInt(32);
            }
            List<ModelStore.ModelPack> packs = store.packs();
            data.writeVarInt(packs.size());
            for (ModelStore.ModelPack pack : packs) {
                data.writeString(pack.folderPath());
                if (pack.iconData() == null) {
                    data.writeVarInt(0);
                } else {
                    data.writeVarInt(1);
                    data.writeByteArray(pack.iconData());
                    data.writeVarInt(pack.iconWidth());
                    data.writeVarInt(pack.iconHeight());
                    data.writeVarInt(2); // PNG
                    data.writeVarInt(1);
                }
                if (pack.name() == null && pack.description() == null) {
                    data.writeVarInt(0);
                } else {
                    data.writeVarInt(1);
                    data.writeString(pack.name() == null ? "" : pack.name());
                    data.writeString(pack.description() == null ? "" : pack.description());
                }
                data.writeVarInt(pack.languages().size());
                for (Map.Entry<String, Map<String, String>> language : pack.languages().entrySet()) {
                    data.writeString(language.getKey());
                    data.writeVarInt(language.getValue().size());
                    for (Map.Entry<String, String> value : language.getValue().entrySet()) {
                        data.writeString(value.getKey());
                        data.writeString(value.getValue());
                    }
                }
            }
            data.writeVarInt(0); // protocol terminator
            YsmCrypt.EncryptedPacket packet = YsmCrypt.encrypt(data.toArray(), session.manifestKey, false);
            send(player, packet.data());
        }
    }

    private void handleRequests(Player player, Session session, byte[] encryptedPayload) throws Exception {
        byte[] decrypted = YsmCrypt.decrypt(encryptedPayload, session.key1);
        if (decrypted == null) throw new IllegalArgumentException("Invalid Sparkle manifest request");
        try (YSMByteBuf data = new YSMByteBuf(Unpooled.wrappedBuffer(decrypted))) {
            data.skipGarbageHeader();
            if (data.readByte() != 4) throw new IllegalArgumentException("Unexpected Sparkle download step");
            int requested = data.readVarInt();
            if (requested < 0 || requested > 4096) throw new IllegalArgumentException("Invalid requested model count");
            for (int index = 0; index < requested; index++) {
                long hash1 = data.readVarLong();
                long hash2 = data.readVarLong();
                sendModel(player, session, hash1, hash2);
            }
        }
        session.step = 3;
    }

    private void sendModel(Player player, Session session, long hash1, long hash2) throws Exception {
        ModelStore.CompiledModel model = store.compiledModels().stream()
                .filter(candidate -> candidate.hash1() == hash1 && candidate.hash2() == hash2)
                .findFirst()
                .orElse(null);
        if (model == null || !Files.isRegularFile(model.cacheFile())) {
            sendTransferFailure(player, session, hash1, hash2, "server_cache_unavailable");
            return;
        }
        byte[] cache = Files.readAllBytes(model.cacheFile());
        if (!YsmCrypt.verifyServerCache(cache, hash1, hash2)) {
            sendTransferFailure(player, session, hash1, hash2, "server_cache_invalid");
            return;
        }
        for (int offset = 0; offset < cache.length; offset += CHUNK_BYTES) {
            int length = Math.min(CHUNK_BYTES, cache.length - offset);
            try (YSMByteBuf data = new YSMByteBuf(Unpooled.buffer(length + 128))) {
                writeGarbage(data);
                data.writeVarInt(5);
                data.writeVarLong(hash1);
                data.writeVarLong(hash2);
                data.writeVarInt(cache.length);
                data.writeVarInt(offset);
                data.writeVarInt(length);
                data.getRawBuf().writeBytes(cache, offset, length);
                YsmCrypt.EncryptedPacket packet = YsmCrypt.encrypt(data.toArray(), session.key1, false);
                send(player, packet.data());
            }
        }
    }

    private void sendTransferFailure(Player player, Session session, long hash1, long hash2, String reason) throws Exception {
        try (YSMByteBuf data = new YSMByteBuf(Unpooled.buffer())) {
            writeGarbage(data);
            data.writeVarInt(6);
            data.writeVarLong(hash1);
            data.writeVarLong(hash2);
            data.writeString(reason);
            YsmCrypt.EncryptedPacket packet = YsmCrypt.encrypt(data.toArray(), session.key1, false);
            send(player, packet.data());
        }
    }

    private void send(Player player, byte[] encryptedPayload) {
        sender.accept(player, SparkleWire.encodeLegacySyncPayload(encryptedPayload));
    }

    private void writeGarbage(YSMByteBuf buffer) {
        int length = 16 + random.nextInt(48);
        byte[] garbage = new byte[length];
        random.nextBytes(garbage);
        buffer.writeGarbageHeader(length, garbage);
    }

    private static byte[] createStableClientKey() {
        byte[] key = new byte[56];
        new Random(114514L).nextBytes(key);
        return key;
    }

    private static final class Session {
        private final byte[] clientKey;
        private byte[] key1;
        private byte[] manifestKey;
        private int step = 1;
        private long lastTouched = System.currentTimeMillis();

        private Session(byte[] clientKey) {
            this.clientKey = clientKey;
        }

        private void touch() {
            lastTouched = System.currentTimeMillis();
        }

        private boolean expired() {
            return System.currentTimeMillis() - lastTouched > SESSION_TIMEOUT_MILLIS;
        }
    }
}
