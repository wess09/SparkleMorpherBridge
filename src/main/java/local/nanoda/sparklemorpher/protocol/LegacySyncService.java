package local.nanoda.sparklemorpher.protocol;

import com.micaftic.morpher.core.security.YSMByteBuf;
import com.micaftic.morpher.core.security.YsmCrypt;
import io.netty.buffer.Unpooled;
import local.nanoda.sparklemorpher.storage.ModelStore;
import org.bukkit.entity.Player;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Server half of Sparkle Morpher's unchanged legacy model-download protocol.
 * The Fabric client intentionally sees the same encrypted payloads as it would
 * from a Fabric server; only packet transport is provided by Bukkit.
 *
 * <p>Downloads are streamed: the producing thread reads, encrypts and enqueues
 * one bounded chunk at a time and pauses once the plugin's per-player outbound
 * backlog exceeds {@code backlogBytes}. The whole model file is never read into
 * memory, so a large library no longer accumulates in an unbounded queue (which
 * previously starved GC on the host and stalled the connection long enough to
 * be kicked for keepalive timeout).
 */
public final class LegacySyncService {
    private static final int CHUNK_BYTES = 30_720;
    /**
     * Hard ceiling for the plaintext manifest frame. The stock client parses the
     * manifest as one decrypted frame per sync step (no chunked-manifest variant
     * exists), and Bukkit rejects a single plugin message over 1 MiB, so pack
     * icons are deferred against this budget. The reserve covers the legacy frame
     * overhead (+1 discriminator byte, +8 trailer hash) and the 1-byte protocol
     * terminator written after the final pack entry.
     */
    private static final int MANIFEST_PLAINTEXT_BUDGET = SparkleWire.PLUGIN_MESSAGE_LIMIT - 9 - 64;
    /** Cheap pre-filter for icon bytes; the encoded frame length is checked exactly before sending. */
    private static final int ICON_FRAME_BUDGET = SparkleWire.PLUGIN_MESSAGE_LIMIT - 2048;
    /** How long the request-drain worker waits for straggler frames before going idle. */
    private static final long REQUEST_GRACE_NANOS = 200_000_000L;
    /** Bound the total model hashes queued per sync session to cap memory from a hostile/looping client. */
    private static final long MAX_PENDING_REQUEST_HASHES = 65_536L;
    /**
     * The stock client indexes its local server cache using this key. It must
     * remain stable across reconnects, otherwise every login looks like a new
     * cache directory and forces a full model redownload.
     */
    private static final byte[] CACHE_CLIENT_KEY = createStableClientKey();

    private final ModelStore store;
    private final BiConsumer<Player, byte[]> sender;
    private final Function<UUID, Player> playerResolver;
    private final ToLongFunction<UUID> pendingBytes;
    private final ExecutorService streamExecutor;
    private final long backlogBytes;
    /**
     * True once the server's model catalog has been fully compiled/restored (boot
     * scan complete). The type-3 manifest is withheld until this is true so a
     * client that connects while models are still being prepared stays on the
     * client's "preparing cache" stage instead of finishing a sync against an
     * empty/partial catalog. Mirrors the official server, which only syncs after
     * its model load completes.
     */
    private final BooleanSupplier catalogReady;
    private final Map<UUID, Session> sessions = new ConcurrentHashMap<>();
    private final SecureRandom random = new SecureRandom();

    public LegacySyncService(ModelStore store, BiConsumer<Player, byte[]> sender,
                             Function<UUID, Player> playerResolver, ToLongFunction<UUID> pendingBytes,
                             ExecutorService streamExecutor, long backlogBytes,
                             BooleanSupplier catalogReady) {
        this.store = store;
        this.sender = sender;
        this.playerResolver = playerResolver;
        this.pendingBytes = pendingBytes;
        this.streamExecutor = streamExecutor;
        this.backlogBytes = Math.max(64 * 1024L, backlogBytes);
        this.catalogReady = catalogReady;
    }

    public void start(Player player) {
        if (player == null || !player.isOnline()) return;
        UUID playerId = player.getUniqueId();
        Session current = sessions.get(playerId);
        if (current != null && current.busy) return; // a download is already running; do not reset it
        Session session = new Session(CACHE_CLIENT_KEY.clone());
        sessions.put(playerId, session);
        try {
            sendHandshake(player, session);
        } catch (Exception error) {
            sessions.remove(playerId, session);
        }
    }

    /** Whether a model download is currently being produced for this player. */
    public boolean isBusy(Player player) {
        if (player == null) return false;
        Session session = sessions.get(player.getUniqueId());
        return session != null && session.busy;
    }

    /** Largest icon byte length deliverable in one id-75 frame (see {@link #ICON_FRAME_BUDGET}). */
    public static int iconFrameBudget() {
        return ICON_FRAME_BUDGET;
    }

    /** Read-only snapshot of a player's sync session for the /spm session diagnostic. */
    public record SessionInfo(boolean present, int step, boolean busy, boolean manifestSent,
                              long queuedHashes, int pendingBatches, boolean packIconsResent, int packIcons) { }

    public SessionInfo describe(Player player) {
        Session session = player == null ? null : sessions.get(player.getUniqueId());
        if (session == null) return new SessionInfo(false, 0, false, false, 0L, 0, false, 0);
        long queued;
        int pending;
        synchronized (session) {
            queued = session.queuedHashes;
            pending = session.pendingRequests.size();
        }
        return new SessionInfo(true, session.step, session.busy, session.manifestSent,
                queued, pending, session.packIconsResent, session.packIcons.size());
    }

    public void accept(Player player, byte[] encryptedPayload) {
        if (player == null || encryptedPayload == null || encryptedPayload.length == 0) return;
        UUID playerId = player.getUniqueId();
        Session session = sessions.get(playerId);
        if (session == null) return;
        try {
            int step = session.step;
            if (step == 1) {
                synchronized (session) {
                    if (session.step == 1) {
                        handlePong(player, session, encryptedPayload);
                    }
                }
            } else if (step == 2 || step == 3) {
                // Model-request phase. The client may split one request across
                // several 0x04 frames (the serverbound payload limit caps how many
                // hashes fit per frame), so instead of dropping everything past the
                // first frame, append each parsed batch to the session and let one
                // drain worker stream them all in arrival order.
                long[][] hashes;
                synchronized (session) {
                    if (session.step != 2 && session.step != 3) return;
                    hashes = parseRequests(player, session, encryptedPayload);
                    if (session.queuedHashes + hashes.length > MAX_PENDING_REQUEST_HASHES) {
                        java.util.logging.Logger.getLogger("SparkleMorpherBridge")
                                .warning("[SM] Rejected model request overflow for " + player.getName()
                                        + " (queued hashes would exceed " + MAX_PENDING_REQUEST_HASHES + ")");
                        return;
                    }
                    session.step = 3;
                    session.pendingRequests.add(hashes);
                    session.queuedHashes += hashes.length;
                    if (session.busy) return; // the running worker will drain it
                    session.busy = true;
                }
                // The client's model request proves it has already processed the
                // manifest (packs are indexed before it builds requests). The icon
                // frames pushed right after the manifest can race AHEAD of the
                // client's asynchronous manifest parsing and be discarded as
                // "unknown pack", so re-push them once the packs are guaranteed
                // indexed. Idempotent; done once per session.
                final List<ModelStore.ModelPack> resendIcons = claimPackIconResend(session);
                if (resendIcons != null) {
                    try {
                        streamExecutor.execute(() -> streamPackIcons(player, session, resendIcons));
                    } catch (RuntimeException ignored) {
                        // pool shutting down; the immediate push may already have landed
                    }
                }
                try {
                    streamExecutor.execute(() -> streamRequests(player, session));
                } catch (RuntimeException error) {
                    session.busy = false;
                    sessions.remove(playerId, session);
                }
            }
        } catch (Exception error) {
            sessions.remove(playerId, session);
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
        session.manifestSent = false;
        if (catalogReady.getAsBoolean()) {
            sendManifest(player, session);
            session.manifestSent = true;
        }
    }

    /**
     * Emits the type-3 manifest for any session that already completed the pong
     * handshake but was parked because the model catalog was not yet ready. Called
     * once the boot scan finishes; idempotent and safe to run concurrently with
     * {@link #accept} (a manifest is only sent once per session).
     */
    public void notifyCatalogReady() {
        for (Map.Entry<UUID, Session> entry : sessions.entrySet()) {
            UUID playerId = entry.getKey();
            Session session = entry.getValue();
            synchronized (session) {
                if (session.step != 2 || session.manifestSent) continue;
            }
            if (sessions.get(playerId) != session) continue; // removed or superseded since
            Player player = playerResolver.apply(playerId);
            if (player == null) continue;
            try {
                synchronized (session) {
                    if (session.step == 2 && !session.manifestSent) {
                        session.manifestSent = true;
                        sendManifest(player, session);
                    }
                }
            } catch (Exception error) {
                sessions.remove(playerId, session);
            }
        }
    }

    private void sendManifest(Player player, Session session) throws Exception {
        List<ModelStore.CompiledModel> models = store.compiledModels();
        List<ModelStore.ModelPack> packs = store.packs();
        int garbageLen = 16 + random.nextInt(48);
        byte[] garbage = new byte[garbageLen];
        random.nextBytes(garbage);

        // The manifest is one legacy frame: the stock client decrypts and parses
        // exactly one frame per sync step (no chunked manifest exists) and Bukkit
        // hard-stops a single plugin message at 1 MiB. Every pack is therefore
        // listed here with METADATA ONLY (no ysm-pack.png) so each category is
        // always visible, and each pack icon is delivered afterwards as its own
        // plaintext id-75 frame — the codec the unmodified client actually uses to
        // patch the icon of an already-listed pack (the official server can embed
        // icons inline only because it is not bound by the 1 MiB plugin-message
        // limit; the bridge is, so it defers them instead of losing covers).
        List<ModelStore.ModelPack> iconPacks = new ArrayList<>();
        for (ModelStore.ModelPack pack : packs) {
            if (pack.iconData() != null && pack.iconData().length > 0) {
                if (pack.iconData().length <= ICON_FRAME_BUDGET) {
                    iconPacks.add(pack);
                } else {
                    java.util.logging.Logger.getLogger("SparkleMorpherBridge")
                            .log(Level.WARNING, "[SM] Pack icon {0} is {1} bytes and cannot fit a single plugin message "
                                    + "(shrink ysm-pack.png to restore the preview).",
                            new Object[]{pack.folderPath(), pack.iconData().length});
                }
            }
        }
        try (YSMByteBuf data = new YSMByteBuf(Unpooled.buffer())) {
            data.writeGarbageHeader(garbageLen, garbage);
            writeManifestPrefix(data, session, models);
            data.writeVarInt(packs.size());
            for (ModelStore.ModelPack pack : packs) {
                writePack(data, pack, false); // metadata only; icon follows as id-75
            }
            data.writeVarInt(0); // protocol terminator
            if (data.getRawBuf().readableBytes() > MANIFEST_PLAINTEXT_BUDGET) {
                java.util.logging.Logger.getLogger("SparkleMorpherBridge")
                        .log(Level.WARNING, "[SM] Model manifest for {0} is {1} bytes: the model catalog alone exceeds the "
                                + "1 MiB plugin-message limit and the sync frame will be refused (needs a chunked catalog "
                                + "protocol beyond this).",
                        new Object[]{player.getName(), data.getRawBuf().readableBytes()});
            }
            YsmCrypt.EncryptedPacket packet = YsmCrypt.encrypt(data.toArray(), session.manifestKey, false);
            send(player, packet.data());
        }
        if (!iconPacks.isEmpty()) {
            session.packIcons = List.copyOf(iconPacks);
            try {
                // Best-effort immediate push; it may race ahead of the client's
                // async manifest parsing and be dropped, so accept() re-pushes once
                // the client's model request proves the packs are indexed.
                streamExecutor.execute(() -> streamPackIcons(player, session, session.packIcons));
            } catch (RuntimeException error) {
                java.util.logging.Logger.getLogger("SparkleMorpherBridge")
                        .log(Level.FINE, "[SM] Could not schedule late pack icons for " + player.getName(), error);
            }
        }
    }

    /** Writes the fixed manifest prefix (garbage header excepted): type 3, folder hash, keys, full model section. */
    private void writeManifestPrefix(YSMByteBuf data, Session session, List<ModelStore.CompiledModel> models) {
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
    }

    /** Exact encoded size in bytes of one pack entry in the requested icon mode. */
    private int packSize(ModelStore.ModelPack pack, boolean withIcon) {
        try (YSMByteBuf probe = new YSMByteBuf(Unpooled.buffer())) {
            writePack(probe, pack, withIcon);
            return probe.getRawBuf().readableBytes();
        }
    }

    /**
     * Sends each pack icon as an id-75 S2CPackIconPacket frame. The pack was already
     * listed (metadata only) in the manifest, so this patches its cover — this is the
     * codec the unmodified client registers for exactly that purpose. The manifest
     * caller (region/control thread) never blocks on flow control.
     */
    private void streamPackIcons(Player player, Session session, List<ModelStore.ModelPack> packs) {
        try {
            for (ModelStore.ModelPack pack : packs) {
                if (!isCurrentSession(player, session)) return;
                awaitBacklogCapacity(player, session);
                if (!isCurrentSession(player, session)) return;
                byte[] icon = pack.iconData();
                if (icon == null || icon.length == 0 || icon.length > ICON_FRAME_BUDGET) continue;
                byte[] frame = SparkleWire.encodePackIcon(pack.folderPath(), icon);
                if (frame.length > SparkleWire.PLUGIN_MESSAGE_LIMIT) continue;
                sender.accept(player, frame);
            }
        } catch (Exception error) {
            sessions.remove(player.getUniqueId(), session);
        }
    }

    /**
     * Returns the icon list to (re)push exactly once per session, or null if it was
     * already pushed or there is nothing to push.
     */
    private static List<ModelStore.ModelPack> claimPackIconResend(Session session) {
        synchronized (session) {
            if (!session.packIconsResent && !session.packIcons.isEmpty()) {
                session.packIconsResent = true;
                return session.packIcons;
            }
            return null;
        }
    }

    private static int varIntBytes(int value) {
        int bytes = 1;
        while ((value & ~0x7F) != 0) {
            value >>>= 7;
            bytes++;
        }
        return bytes;
    }

    /** Writes one pack manifest entry; icons are optional per the legacy protocol. */
    private static void writePack(YSMByteBuf data, ModelStore.ModelPack pack, boolean withIcon) {
        data.writeString(pack.folderPath());
        if (!withIcon || pack.iconData() == null) {
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

    /**
     * Decrypts the client's request list and returns the requested model hash
     * pairs. Small, so it runs directly on the control thread.
     */
    private long[][] parseRequests(Player player, Session session, byte[] encryptedPayload) throws Exception {
        byte[] decrypted = YsmCrypt.decrypt(encryptedPayload, session.key1);
        if (decrypted == null) throw new IllegalArgumentException("Invalid Sparkle manifest request");
        try (YSMByteBuf data = new YSMByteBuf(Unpooled.wrappedBuffer(decrypted))) {
            data.skipGarbageHeader();
            if (data.readByte() != 4) throw new IllegalArgumentException("Unexpected Sparkle download step");
            int requested = data.readVarInt();
            if (requested < 0 || requested > 4096) throw new IllegalArgumentException("Invalid requested model count");
            // 诊断探针:记录一次模型请求的规模与密文大小,用于核对被踢是否由"超大单帧请求"引起。
            java.util.logging.Logger.getLogger("SparkleMorpherBridge")
                    .info("[SM] Model request from " + player.getName() + ": " + requested
                            + " models, encrypted payload " + encryptedPayload.length + "B");
            long[][] hashes = new long[requested][2];
            for (int index = 0; index < requested; index++) {
                hashes[index][0] = data.readVarLong();
                hashes[index][1] = data.readVarLong();
            }
            return hashes;
        }
    }

    /**
     * Produces every requested model as a bounded stream on the download pool.
     * Drains request batches queued for the session; batches may still be in
     * flight when the queue first empties (the client splits one request across
     * several frames), so the worker parks briefly and only stops when a grace
     * scan finds the queue still empty.
     */
    private void streamRequests(Player player, Session session) {
        try {
            while (true) {
                long[][] batch;
                synchronized (session) {
                    batch = session.pendingRequests.poll();
                    if (batch != null) {
                        session.queuedHashes -= batch.length;
                        if (session.queuedHashes < 0) session.queuedHashes = 0;
                    }
                }
                if (batch != null) {
                    List<ModelStore.CompiledModel> compiled = store.compiledModels();
                    for (long[] pair : batch) {
                        if (!isCurrentSession(player, session)) return;
                        streamModel(player, session, pair[0], pair[1], compiled);
                    }
                    continue;
                }
                if (!isCurrentSession(player, session)) return;
                LockSupport.parkNanos(REQUEST_GRACE_NANOS);
                boolean done;
                synchronized (session) {
                    done = session.pendingRequests.isEmpty();
                    if (done) session.busy = false;
                }
                if (done) return;
            }
        } catch (Exception error) {
            sessions.remove(player.getUniqueId(), session);
        } finally {
            synchronized (session) {
                session.busy = false;
            }
        }
    }

    private void streamModel(Player player, Session session, long hash1, long hash2,
                             List<ModelStore.CompiledModel> compiled) throws Exception {
        ModelStore.CompiledModel model = compiled.stream()
                .filter(candidate -> candidate.hash1() == hash1 && candidate.hash2() == hash2)
                .findFirst()
                .orElse(null);
        if (model == null || !Files.isRegularFile(model.cacheFile())) {
            sendTransferFailure(player, session, hash1, hash2, "server_cache_unavailable");
            return;
        }
        long total = Files.size(model.cacheFile());
        if (total <= 0L || total > Integer.MAX_VALUE) {
            sendTransferFailure(player, session, hash1, hash2, "server_cache_invalid");
            return;
        }
        // Server-cache files are written by publishCompiled from the exact bytes
        // the hashes were derived from, so no whole-file verification pass is
        // needed here; the stock client re-verifies on its final chunk anyway.
        byte[] chunk = new byte[CHUNK_BYTES];
        int offset = 0;
        int totalBytes = (int) total;
        try (RandomAccessFile file = new RandomAccessFile(model.cacheFile().toFile(), "r")) {
            while (offset < totalBytes) {
                if (!isCurrentSession(player, session)) return;
                awaitBacklogCapacity(player, session);
                if (!isCurrentSession(player, session)) return;
                int length = Math.min(CHUNK_BYTES, totalBytes - offset);
                file.seek(offset);
                file.readFully(chunk, 0, length);
                try (YSMByteBuf data = new YSMByteBuf(Unpooled.buffer(length + 128))) {
                    writeGarbage(data);
                    data.writeVarInt(5);
                    data.writeVarLong(hash1);
                    data.writeVarLong(hash2);
                    data.writeVarInt(totalBytes);
                    data.writeVarInt(offset);
                    data.writeVarInt(length);
                    data.getRawBuf().writeBytes(chunk, 0, length);
                    YsmCrypt.EncryptedPacket packet = YsmCrypt.encrypt(data.toArray(), session.key1, false);
                    send(player, packet.data());
                }
                offset += length;
            }
        }
    }

    /** Pause production until this player's queued-but-unsent backlog is under budget. */
    private void awaitBacklogCapacity(Player player, Session session) {
        UUID playerId = player.getUniqueId();
        while (pendingBytes.applyAsLong(playerId) >= backlogBytes) {
            if (!isCurrentSession(player, session)) return;
            LockSupport.parkNanos(2_000_000L);
        }
    }

    /** True while the session is still the current one and its connection is alive. */
    private boolean isCurrentSession(Player player, Session session) {
        UUID playerId = player.getUniqueId();
        if (sessions.get(playerId) != session) return false;
        Player current = playerResolver.apply(playerId);
        return current != null && current.isOnline();
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
        /** Model-request batches (one per received 0x04 frame) awaiting streaming. */
        private final ConcurrentLinkedQueue<long[][]> pendingRequests = new ConcurrentLinkedQueue<>();
        /** Total hashes currently queued in {@link #pendingRequests}; guarded by the session monitor. */
        private long queuedHashes;
        private volatile byte[] key1;
        private volatile byte[] manifestKey;
        private volatile int step = 1;
        private volatile boolean busy;
        /** Whether the type-3 manifest has already been emitted for this session. */
        private volatile boolean manifestSent;
        /** Packs whose icons must be (re)pushed to this client; set when the manifest is sent. */
        private volatile List<ModelStore.ModelPack> packIcons = List.of();
        /** Whether the post-request icon re-push has already run for this session. */
        private volatile boolean packIconsResent;

        private Session(byte[] clientKey) {
            this.clientKey = clientKey;
        }
    }
}
