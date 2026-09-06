package local.nanoda.sparklemorpher.storage;

import com.micaftic.morpher.core.security.YSMByteBuf;
import com.micaftic.morpher.core.security.YsmCrypt;
import com.micaftic.morpher.resource.YSMBinaryDeserializer;
import com.micaftic.morpher.resource.YSMBinarySerializer;
import com.micaftic.morpher.resource.YSMFolderDeserializer;
import com.micaftic.morpher.resource.pojo.RawYsmModel;

import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.Arrays;

/** Turns official Sparkle .ysm releases into the byte stream used by remote clients. */
public final class ModelCompiler {
    private final ModelStore store;

    public ModelCompiler(ModelStore store) {
        this.store = store;
    }

    public ModelStore.CompiledModel compile(ModelStore.ModelFile model) throws Exception {
        if (model == null) throw new IllegalArgumentException("Uploaded model is missing");
        String lowerName = model.fileName().toLowerCase(java.util.Locale.ROOT);
        byte[] source = Files.readAllBytes(model.path());
        RawYsmModel rawModel;
        if (lowerName.endsWith(".ysm")) {
            byte[] clear = YsmCrypt.decryptYsmFile(source);
            try (YSMBinaryDeserializer deserializer = new YSMBinaryDeserializer(clear)) {
                rawModel = deserializer.deserializeKeepOpen();
            }
        } else if (lowerName.endsWith(".zip")) {
            try (YSMFolderDeserializer deserializer = new YSMFolderDeserializer(model.path())) {
                rawModel = deserializer.deserialize();
            }
        } else {
            throw new IllegalArgumentException("Unsupported release package; expected .ysm or .zip");
        }
        // A significant number of legacy 2.5/2.6 exports omit the optional
        // model hash. The original bytes are stable, so they are a suitable
        // cache identity and keep these packages distributable.
        String modelHash = rawModel.properties.sha256;
        if (modelHash == null || !modelHash.matches("[0-9a-fA-F]{64}")) {
            modelHash = sha256(source);
        }
        // ServerModelInfo derives its runtime variable key from this field.
        // Official exports contain it; legacy exports that omit it need the
        // stable source-derived equivalent before the server cache is written.
        rawModel.properties.sha256 = modelHash;

        byte[] serialized;
        try (YSMByteBuf serializedBuffer = YSMBinarySerializer.serialize(rawModel, 32, true)) {
            io.netty.buffer.ByteBuf data = serializedBuffer.getRawBuf();
            if (data.hasArray()) {
                int offset = data.arrayOffset() + data.readerIndex();
                serialized = Arrays.copyOfRange(data.array(), offset, offset + data.readableBytes());
            } else {
                serialized = serializedBuffer.toArray();
            }
        }
        return store.publishCompiled(model.modelId(), modelHash, serialized, false);
    }

    private static String sha256(byte[] data) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
            StringBuilder value = new StringBuilder(64);
            for (byte item : digest) value.append(String.format("%02x", item));
            return value.toString();
        } catch (Exception error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }
}
