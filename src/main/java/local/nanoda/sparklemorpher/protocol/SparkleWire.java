package local.nanoda.sparklemorpher.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Sparkle Morpher Fabric 26.1.2 custom-payload framing. */
public final class SparkleWire {
    public static final String CHANNEL = "sparkle_morpher:2_6_0";
    public static final String BRIDGE_CHANNEL = "sparkle_morpher:folia_bridge_v1";
    /** Bukkit rejects single plugin messages over this size (StandardMessenger.validatePluginMessage). */
    public static final int PLUGIN_MESSAGE_LIMIT = 1_048_576;
    public static final String VERSION = "2.6.0";
    public static final int S2C_AUTH_MODELS = 6;
    public static final int S2C_EXECUTE_MOLANG = 3;
    public static final int S2C_MODEL_STATE = 4;
    public static final int C2S_COMPLETE_FEEDBACK = 15;
    public static final int C2S_REQUEST_EXECUTE_MOLANG = 17;
    public static final int S2C_STAR_MODELS = 8;
    public static final int C2S_SET_STAR_MODEL = 9;
    public static final int C2S_PLAY_ANIMATION = 7;
    public static final int C2S_ANIMATION_EXPRESSION = 18;
    public static final int C2S_SWING_ARM = 23;
    public static final int S2C_ANIMATION_EXPRESSION = 19;
    public static final int S2C_PLAYER_STATE = 21;
    /** Late-delivered pack icon (PNG), one per frame; clients without this codec ignore it. */
    public static final int S2C_PACK_ICON = 75;
    /**
     * Late-delivered full pack entry (metadata + optional PNG). The legacy
     * manifest is capped at 1 MiB, so on very large libraries whole pack entries
     * cannot fit it; newer clients upsert them from these frames.
     */
    public static final int S2C_PACK_ENTRY = 76;
    public static final int S2C_VERSION_CHECK = 51;
    public static final int C2S_VERSION_CHECK = 52;
    public static final int C2S_SELECT_MODEL = 5;
    public static final int C2S_UPLOAD_START = 70;
    public static final int S2C_UPLOAD_START = 71;
    public static final int C2S_UPLOAD_CHUNK = 72;
    public static final int C2S_UPLOAD_FINISH = 73;
    public static final int S2C_UPLOAD_RESULT = 74;
    /** S2CSyncPlayerStatePacket field-presence bits (see encodePlayerStateFlying/encodePlayerStateFood). */
    public static final int S2C_PLAYER_STATE_FLAG_FULL_SYNC = 0x0001;
    public static final int S2C_PLAYER_STATE_FLAG_FLYING = 0x0002;
    public static final int S2C_PLAYER_STATE_FLAG_FOOD = 0x0010;
    public static final int S2C_PLAYER_STATE_FLAG_MODEL_SWITCH = 0x0800;
    public static final int S2C_PLAYER_STATE_FLAG_MOLANG = 0x1000;
    private static final int MAX_VERSION_BYTES = 64;

    private SparkleWire() {
    }

    public sealed interface ClientMessage permits VersionCheck, ModelSelection, CompleteFeedback, RequestExecuteMolang, SetStarModel, PlayAnimation, AnimationExpression, SwingArm, UploadStart, UploadChunk, UploadFinish, LegacySyncPayload {
    }

    public record VersionCheck(String version) implements ClientMessage {
    }

    public record ModelSelection(String modelId, String textureId) implements ClientMessage {
    }

    /** C2SCompleteFeedbackPacket: runtime model variables, used by synced wardrobe states. */
    public record CompleteFeedback(int modelHashId, int entityId, Map<String, Float> values) implements ClientMessage {
    }

    public record RequestExecuteMolang(String expression, int entityId) implements ClientMessage {
    }

    public record SetStarModel(String modelId, boolean add) implements ClientMessage {
    }

    public record PlayAnimation(int animationIndex, String category, int entityId, String animationKey) implements ClientMessage {
    }

    public record AnimationExpression(float[] values) implements ClientMessage {
    }

    public record SwingArm(boolean offHand) implements ClientMessage {
    }

    public record UploadStart(String modelId, String fileName, int totalBytes, String sha256) implements ClientMessage {
    }

    public record UploadChunk(long uploadId, int offset, byte[] data) implements ClientMessage {
    }

    public record UploadFinish(long uploadId) implements ClientMessage {
    }

    /** The legacy client sends an encrypted, unframed payload after discriminator 2. */
    public record LegacySyncPayload(byte[] data) implements ClientMessage {
    }

    public static ClientMessage decodeClientMessage(byte[] body) {
        Reader reader = new Reader(body);
        int id = reader.readUnsignedByte();
        ClientMessage message = switch (id) {
            case C2S_VERSION_CHECK -> new VersionCheck(reader.readUtf(MAX_VERSION_BYTES));
            case 2 -> new LegacySyncPayload(reader.readRemaining());
            case C2S_SELECT_MODEL -> new ModelSelection(reader.readUtf(256), reader.readUtf(256));
            case C2S_COMPLETE_FEEDBACK -> new CompleteFeedback(reader.readInt(), reader.readVarInt(), reader.readFloatMap(127));
            case C2S_REQUEST_EXECUTE_MOLANG -> new RequestExecuteMolang(reader.readUtf(2_048), reader.readVarInt());
            case C2S_SET_STAR_MODEL -> new SetStarModel(reader.readUtf(256), reader.readBoolean());
            case C2S_PLAY_ANIMATION -> new PlayAnimation(reader.readVarInt(), reader.readUtf(256), reader.readVarInt(), reader.readUtf(512));
            case C2S_ANIMATION_EXPRESSION -> new AnimationExpression(reader.readFloatArray(64));
            case C2S_SWING_ARM -> new SwingArm(reader.readUnsignedByte() == 1);
            case C2S_UPLOAD_START -> new UploadStart(reader.readUtf(256), reader.readUtf(256), reader.readVarInt(), reader.readUtf(128));
            case C2S_UPLOAD_CHUNK -> new UploadChunk(reader.readVarLong(), reader.readVarInt(), reader.readByteArray(33_000));
            case C2S_UPLOAD_FINISH -> new UploadFinish(reader.readVarLong());
            default -> throw new IllegalArgumentException("Unsupported client packet id " + id);
        };
        if (reader.hasRemaining()) {
            throw new IllegalArgumentException("Unexpected trailing client data");
        }
        return message;
    }

    public static byte[] encodeServerVersionCheck(boolean allowUpload) {
        Writer writer = new Writer();
        writer.writeByte(S2C_VERSION_CHECK);
        writer.writeUtf(VERSION);
        writer.writeUtf("open_ysm:v1");
        writer.writeBoolean(allowUpload);
        return writer.toByteArray();
    }

    public static byte[] encodeEmptyAuthorizationList() {
        Writer writer = new Writer();
        writer.writeByte(S2C_AUTH_MODELS);
        writer.writeVarInt(0);
        return writer.toByteArray();
    }

    public static byte[] encodeStarModels(Set<String> modelIds) {
        Writer writer = new Writer();
        writer.writeByte(S2C_STAR_MODELS);
        writer.writeVarInt(modelIds.size());
        for (String modelId : modelIds) writer.writeUtf(modelId);
        return writer.toByteArray();
    }

    public static byte[] encodeExecuteMolang(int entityId, String expression) {
        Writer writer = new Writer();
        writer.writeByte(S2C_EXECUTE_MOLANG);
        writer.writeVarInt(1);
        writer.writeVarInt(entityId);
        writer.writeUtf(expression);
        return writer.toByteArray();
    }

    public static byte[] encodeUploadStart(long uploadId, byte status, int chunkSize, int maxBytes, int chunksPerTick, String message) {
        Writer writer = new Writer();
        writer.writeByte(S2C_UPLOAD_START);
        writer.writeVarLong(uploadId);
        writer.writeByte(status);
        writer.writeVarInt(chunkSize);
        writer.writeVarInt(maxBytes);
        writer.writeVarInt(chunksPerTick);
        writer.writeUtf(message == null ? "" : message);
        return writer.toByteArray();
    }

    public static byte[] encodeUploadResult(long uploadId, byte status, String modelId, String message) {
        return encodeUploadResult(uploadId, status, modelId, 0L, 0L, message);
    }

    public static byte[] encodeUploadResult(long uploadId, byte status, String modelId, long hash1, long hash2, String message) {
        Writer writer = new Writer();
        writer.writeByte(S2C_UPLOAD_RESULT);
        writer.writeVarLong(uploadId);
        writer.writeByte(status);
        writer.writeUtf(modelId == null ? "" : modelId);
        writer.writeVarLong(hash1);
        writer.writeVarLong(hash2);
        writer.writeUtf(message == null ? "" : message);
        return writer.toByteArray();
    }

    public static byte[] encodeModelState(int entityId, String modelId, String textureId) {
        return encodeModelState(entityId, modelId, textureId, null, false, -1);
    }

    /** S2CSetModelAndTexture with a full Molang state snapshot when present. */
    public static byte[] encodeModelState(int entityId, String modelId, String textureId, MolangState molang) {
        return encodeModelState(entityId, modelId, textureId, molang, false, -1);
    }

    /**
     * S2CSetModelAndTexture with the caller's current entity-state snapshot embedded.
     * The official server always attaches its full S2CSyncPlayerStatePacket to model
     * frames, so a watcher that first sees a player learns its flight/hunger state
     * immediately instead of waiting for a later delta. {@code flying} contributes
     * the flight bit; a non-negative {@code foodLevel} contributes the hunger bit.
     */
    public static byte[] encodeModelState(int entityId, String modelId, String textureId, MolangState molang,
                                          boolean flying, int foodLevel) {
        Writer writer = new Writer();
        writer.writeByte(S2C_MODEL_STATE);
        writer.writeVarInt(entityId);
        writer.writeUtf(modelId);
        writer.writeUtf(textureId == null ? "" : textureId);
        writer.writeBoolean(false);
        writer.writeVarInt(entityId);
        boolean hasMolang = molang != null && !molang.values().isEmpty();
        int flags = 0;
        if (flying) flags |= S2C_PLAYER_STATE_FLAG_FLYING;
        if (foodLevel >= 0) flags |= S2C_PLAYER_STATE_FLAG_FOOD;
        if (hasMolang) flags |= S2C_PLAYER_STATE_FLAG_FULL_SYNC | S2C_PLAYER_STATE_FLAG_MOLANG;
        writer.writeShort(flags);
        if (flying) writer.writeBoolean(true);
        if (foodLevel >= 0) writer.writeVarInt(Math.max(0, foodLevel));
        if (hasMolang) {
            writer.writeInt(molang.modelHashId());
            writer.writeVarInt(molang.values().size());
            molang.values().forEach((name, value) -> {
                writer.writeUtf(name);
                writer.writeFloat(value);
            });
        }
        return writer.toByteArray();
    }

    public record MolangState(int modelHashId, Map<String, Float> values) {
    }

    /** S2CSyncPlayerStatePacket containing only the extra-animation selector. */
    public static byte[] encodeAnimationState(int entityId, String animationKey) {
        Writer writer = new Writer();
        writer.writeByte(S2C_PLAYER_STATE);
        writer.writeVarInt(entityId);
        writer.writeShort(2048);
        writer.writeUtf(animationKey == null ? "" : animationKey);
        return writer.toByteArray();
    }

    /** S2CSyncAnimationExpressionPacket. */
    public static byte[] encodeAnimationExpression(int entityId, float[] values) {
        if (values == null || values.length > 64) throw new IllegalArgumentException("Invalid animation expression");
        Writer writer = new Writer();
        writer.writeByte(S2C_ANIMATION_EXPRESSION);
        writer.writeVarInt(entityId);
        writer.writeByte(values.length);
        for (float value : values) writer.writeFloat(value);
        return writer.toByteArray();
    }

    /** S2CSyncPlayerStatePacket carrying only the flight-state change. */
    public static byte[] encodePlayerStateFlying(int entityId, boolean flying) {
        Writer writer = new Writer();
        writer.writeByte(S2C_PLAYER_STATE);
        writer.writeVarInt(entityId);
        writer.writeShort(S2C_PLAYER_STATE_FLAG_FLYING);
        writer.writeBoolean(flying);
        return writer.toByteArray();
    }

    /** S2CSyncPlayerStatePacket carrying only the hunger-level change. */
    public static byte[] encodePlayerStateFood(int entityId, int foodLevel) {
        Writer writer = new Writer();
        writer.writeByte(S2C_PLAYER_STATE);
        writer.writeVarInt(entityId);
        writer.writeShort(S2C_PLAYER_STATE_FLAG_FOOD);
        writer.writeVarInt(Math.max(0, foodLevel));
        return writer.toByteArray();
    }

    /** S2CSyncPlayerStatePacket Molang delta, matching ModelInfoCapability.applyFeedback. */
    public static byte[] encodeMolangState(int entityId, int modelHashId, Map<String, Float> values) {
        Writer writer = new Writer();
        writer.writeByte(S2C_PLAYER_STATE);
        writer.writeVarInt(entityId);
        writer.writeShort(4096);
        writer.writeInt(modelHashId);
        writer.writeVarInt(values.size());
        values.forEach((name, value) -> {
            writer.writeUtf(name);
            writer.writeFloat(value);
        });
        return writer.toByteArray();
    }

    public static byte[] encodeLegacySyncPayload(byte[] encryptedData) {
        if (encryptedData == null || encryptedData.length == 0) {
            throw new IllegalArgumentException("Missing legacy sync data");
        }
        Writer writer = new Writer();
        writer.writeByte(1);
        writer.writeBytes(encryptedData);
        return writer.toByteArray();
    }

    /**
     * Late pack-icon patch: replaces the icon of an already-listed pack. Plaintext
     * PNG passthrough (the legacy manifest carried width/height/imageFormat only
     * for its non-PNG encodings). Single frame must stay under the plugin-message
     * limit, so the caller skips icons that cannot fit.
     */
    public static byte[] encodePackIcon(String folderPath, byte[] png) {
        if (folderPath == null || folderPath.length() > 256) {
            throw new IllegalArgumentException("Invalid pack folder path");
        }
        if (png == null || png.length == 0) {
            throw new IllegalArgumentException("Missing pack icon");
        }
        Writer writer = new Writer();
        writer.writeByte(S2C_PACK_ICON);
        writer.writeUtf(folderPath);
        writer.writeVarInt(png.length);
        writer.writeBytes(png);
        return writer.toByteArray();
    }

    /**
     * Full pack entry patch (id 76): folderPath, optional PNG icon, display
     * name/description and translations. The client inserts it when the pack was
     * not present in the manifest (dropped on a huge library) or replaces the
     * manifest's icon-less entry with the icon included.
     */
    public static byte[] encodePackEntry(String folderPath, byte[] icon, String name, String description,
                                         Map<String, Map<String, String>> languages) {
        if (folderPath == null || folderPath.length() > 256) {
            throw new IllegalArgumentException("Invalid pack folder path");
        }
        Writer writer = new Writer();
        writer.writeByte(S2C_PACK_ENTRY);
        writer.writeUtf(folderPath);
        boolean hasIcon = icon != null && icon.length > 0;
        writer.writeBoolean(hasIcon);
        if (hasIcon) {
            writer.writeVarInt(icon.length);
            writer.writeBytes(icon);
        }
        writer.writeUtf(name == null ? "" : name);
        writer.writeUtf(description == null ? "" : description);
        Map<String, Map<String, String>> entries = languages == null ? Map.of() : languages;
        writer.writeVarInt(entries.size());
        for (Map.Entry<String, Map<String, String>> language : entries.entrySet()) {
            writer.writeUtf(language.getKey());
            Map<String, String> values = language.getValue() == null ? Map.of() : language.getValue();
            writer.writeVarInt(values.size());
            for (Map.Entry<String, String> value : values.entrySet()) {
                writer.writeUtf(value.getKey());
                writer.writeUtf(value.getValue() == null ? "" : value.getValue());
            }
        }
        return writer.toByteArray();
    }

    private static final class Reader {
        private final ByteBuffer buffer;

        private Reader(byte[] body) {
            if (body == null) {
                throw new IllegalArgumentException("Missing payload");
            }
            buffer = ByteBuffer.wrap(body);
        }

        private int readUnsignedByte() {
            require(1);
            return Byte.toUnsignedInt(buffer.get());
        }

        private String readUtf(int maximumBytes) {
            int length = readVarInt();
            if (length < 0 || length > maximumBytes) {
                throw new IllegalArgumentException("Invalid UTF length " + length);
            }
            require(length);
            byte[] content = new byte[length];
            buffer.get(content);
            return new String(content, StandardCharsets.UTF_8);
        }

        private int readVarInt() {
            int value = 0;
            for (int position = 0; position < 5; position++) {
                int current = readUnsignedByte();
                value |= (current & 0x7f) << (position * 7);
                if ((current & 0x80) == 0) {
                    return value;
                }
            }
            throw new IllegalArgumentException("VarInt exceeds 5 bytes");
        }

        private long readVarLong() {
            long value = 0;
            for (int position = 0; position < 10; position++) {
                int current = readUnsignedByte();
                value |= (long) (current & 0x7f) << (position * 7);
                if ((current & 0x80) == 0) {
                    return value;
                }
            }
            throw new IllegalArgumentException("VarLong exceeds 10 bytes");
        }

        private int readInt() {
            require(4);
            return buffer.getInt();
        }

        private byte[] readByteArray(int maximumBytes) {
            int length = readVarInt();
            if (length < 0 || length > maximumBytes) {
                throw new IllegalArgumentException("Invalid byte array length " + length);
            }
            require(length);
            byte[] data = new byte[length];
            buffer.get(data);
            return data;
        }

        private byte[] readRemaining() {
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            return data;
        }

        private boolean readBoolean() {
            return readUnsignedByte() != 0;
        }

        private float[] readFloatArray(int maximumValues) {
            int length = readUnsignedByte();
            if (length > maximumValues) throw new IllegalArgumentException("Too many animation expression values");
            float[] values = new float[length];
            for (int index = 0; index < length; index++) {
                require(4);
                values[index] = buffer.getFloat();
            }
            return values;
        }

        private Map<String, Float> readFloatMap(int maximumEntries) {
            int length = readUnsignedByte();
            if (length > maximumEntries) throw new IllegalArgumentException("Too many model variables");
            Map<String, Float> values = new LinkedHashMap<>(length);
            for (int index = 0; index < length; index++) {
                String key = readUtf(128);
                require(4);
                values.put(key, buffer.getFloat());
            }
            return Map.copyOf(values);
        }

        private boolean hasRemaining() {
            return buffer.hasRemaining();
        }

        private void require(int length) {
            if (buffer.remaining() < length) {
                throw new IllegalArgumentException("Truncated payload");
            }
        }
    }

    private static final class Writer {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();

        private void writeByte(int value) {
            output.write(value & 0xff);
        }

        private void writeBoolean(boolean value) {
            writeByte(value ? 1 : 0);
        }

        private void writeUtf(String value) {
            byte[] content = value.getBytes(StandardCharsets.UTF_8);
            writeVarInt(content.length);
            output.writeBytes(content);
        }

        private void writeVarInt(int value) {
            if (value < 0) {
                throw new IllegalArgumentException("Negative VarInt");
            }
            do {
                int current = value & 0x7f;
                value >>>= 7;
                if (value != 0) {
                    current |= 0x80;
                }
                writeByte(current);
            } while (value != 0);
        }

        private void writeVarLong(long value) {
            do {
                int current = (int) (value & 0x7f);
                value >>>= 7;
                if (value != 0) {
                    current |= 0x80;
                }
                writeByte(current);
            } while (value != 0);
        }

        private void writeShort(int value) {
            writeByte(value >>> 8);
            writeByte(value);
        }

        private void writeFloat(float value) {
            int bits = Float.floatToIntBits(value);
            writeByte(bits >>> 24);
            writeByte(bits >>> 16);
            writeByte(bits >>> 8);
            writeByte(bits);
        }

        private void writeInt(int value) {
            writeByte(value >>> 24);
            writeByte(value >>> 16);
            writeByte(value >>> 8);
            writeByte(value);
        }

        private void writeBytes(byte[] value) {
            output.writeBytes(value);
        }

        private byte[] toByteArray() {
            return output.toByteArray();
        }
    }
}
