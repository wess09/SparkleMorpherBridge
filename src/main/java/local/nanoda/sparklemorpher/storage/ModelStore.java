package local.nanoda.sparklemorpher.storage;

import com.micaftic.morpher.core.security.YsmCrypt;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.bukkit.configuration.file.YamlConfiguration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.Base64;

/** Persistent server model storage plus bounded, ordered upload sessions. */
public final class ModelStore {
    private static final Pattern INVALID_MODEL_ID_CHARS = Pattern.compile("[^\\p{L}\\p{M}\\p{N}_./-]+");
    private static final Pattern MODEL_ID_PATTERN = Pattern.compile("[\\p{L}\\p{M}\\p{N}_./-]+");
    private static final Pattern MODEL_ID_CONTENT_PATTERN = Pattern.compile(".*[\\p{L}\\p{N}].*");
    public record ModelFile(String modelId, String fileName, Path path, long size, String sha256) {
    }

    public record UploadStart(long id, byte status, String message) {
    }

    public record UploadResult(long id, byte status, String modelId, String message) {
    }

    /** Metadata for a model already transformed into Sparkle's server-cache format. */
    public record CompiledModel(String modelId, String modelHash, long hash1, long hash2, Path cacheFile, boolean auth) {
        public int modelHashId() {
            return Integer.parseUnsignedInt(modelHash.substring(0, 8), 16);
        }
    }

    public record Selection(String modelId, String textureId) {
    }

    public record MolangState(int modelHashId, Map<String, Float> values) {
    }

    /** Direct equivalent of the official ysm-pack.json manifest data. */
    public record ModelPack(String folderPath, byte[] iconData, int iconWidth, int iconHeight,
                            String name, String description, Map<String, Map<String, String>> languages) {
    }

    private record Upload(UUID owner, String requestedId, String modelId, String fileName, String sha256,
                          byte[] data, long lastTouched, int received) {
        Upload touch() { return new Upload(owner, requestedId, modelId, fileName, sha256, data, System.currentTimeMillis(), received); }
        Upload append(int offset, byte[] chunk) {
            if (chunk == null || offset != received || offset < 0 || offset + chunk.length > data.length) return null;
            System.arraycopy(chunk, 0, data, offset, chunk.length);
            return new Upload(owner, requestedId, modelId, fileName, sha256, data, System.currentTimeMillis(), received + chunk.length);
        }
    }

    private final Path modelsDirectory;
    private final Path cacheDirectory;
    private final Path selectionsFile;
    private final Path playerStateFile;
    private final Path starsFile;
    private final Path catalogFile;
    private final Path serverKeyFile;
    private final Map<Long, Upload> uploads = new ConcurrentHashMap<>();
    private final Map<UUID, Selection> selections = new ConcurrentHashMap<>();
    private final Map<UUID, MolangState> molangStates = new ConcurrentHashMap<>();
    private final Map<UUID, Set<String>> stars = new ConcurrentHashMap<>();
    private final Map<String, CompiledModel> compiledModels = new ConcurrentHashMap<>();
    private final SecureRandom random = new SecureRandom();
    private final int maxBytes;
    private final long timeoutMillis;
    private final byte[] serverKey;

    public ModelStore(Path dataDirectory, int maxBytes, int timeoutSeconds) throws IOException {
        this.modelsDirectory = dataDirectory.resolve("models").toAbsolutePath().normalize();
        this.cacheDirectory = dataDirectory.resolve("server-cache").toAbsolutePath().normalize();
        this.selectionsFile = dataDirectory.resolve("selections.yml");
        this.playerStateFile = dataDirectory.resolve("player-state.yml");
        this.starsFile = dataDirectory.resolve("stars.yml");
        this.catalogFile = dataDirectory.resolve("catalog.yml");
        this.serverKeyFile = dataDirectory.resolve("server-key.bin");
        this.maxBytes = maxBytes;
        this.timeoutMillis = timeoutSeconds * 1000L;
        Files.createDirectories(modelsDirectory);
        Files.createDirectories(cacheDirectory);
        this.serverKey = loadOrCreateServerKey();
        loadSelections();
        loadPlayerStates();
        loadStars();
        loadCatalog();
    }

    public UploadStart begin(UUID owner, boolean permitted, String requestedId, String fileName, int totalBytes, String sha256) {
        expireUploads();
        String localId = normalizeId(requestedId);
        String extension = extension(fileName);
        if (!permitted) return new UploadStart(0, (byte) 3, "No import permission");
        if (localId == null || extension.isEmpty() || sha256 == null || !sha256.matches("[0-9a-fA-F]{64}")) {
            return new UploadStart(0, (byte) 5, "Invalid model id or hash");
        }
        if (totalBytes <= 0 || totalBytes > maxBytes) return new UploadStart(0, (byte) 2, "File exceeds server limit");
        String modelId = localId;
        Path target = target(modelId, extension);
        if (Files.exists(target) || uploads.values().stream().anyMatch(upload -> upload.modelId.equals(modelId))) {
            return new UploadStart(0, (byte) 1, "Model ID already exists");
        }
        long id;
        do { id = random.nextLong(); } while (id == 0 || uploads.containsKey(id));
        uploads.put(id, new Upload(owner, localId, modelId, safeFileName(localId, extension), sha256.toLowerCase(Locale.ROOT), new byte[totalBytes], System.currentTimeMillis(), 0));
        return new UploadStart(id, (byte) 0, "");
    }

    public boolean append(UUID owner, long uploadId, int offset, byte[] chunk) {
        Upload current = uploads.get(uploadId);
        if (current == null || !current.owner.equals(owner)) return false;
        Upload updated = current.append(offset, chunk);
        if (updated == null) {
            uploads.remove(uploadId, current);
            return false;
        }
        return uploads.replace(uploadId, current, updated);
    }

    public UploadResult finish(UUID owner, long uploadId) {
        Upload upload = uploads.remove(uploadId);
        if (upload == null || !upload.owner.equals(owner)) return new UploadResult(uploadId, (byte) 4, "", "Session expired");
        if (upload.received != upload.data.length) return new UploadResult(uploadId, (byte) 5, "", "Incomplete upload");
        if (!upload.sha256.equals(sha256(upload.data))) return new UploadResult(uploadId, (byte) 1, "", "Hash mismatch");
        try {
            Path target = target(upload.modelId, extension(upload.fileName));
            Files.createDirectories(target.getParent());
            Path temporary = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
            Files.write(temporary, upload.data);
            try {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException ignored) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
            return new UploadResult(uploadId, (byte) 0, upload.modelId, "");
        } catch (IOException error) {
            return new UploadResult(uploadId, (byte) 3, "", "Server storage error");
        }
    }

    public List<ModelFile> models() {
        List<ModelFile> models = new ArrayList<>();
        try (var stream = Files.walk(modelsDirectory)) {
            stream.filter(Files::isRegularFile).forEach(path -> {
                try {
                    String relative = modelsDirectory.relativize(path).toString().replace('\\', '/');
                    String extension = extension(relative);
                    if (extension.isEmpty()) return;
                    String baseId = relative.substring(0, relative.length() - extension.length());
                    String id = normalizeId(baseId);
                    if (id != null) {
                        models.add(new ModelFile(id, relative, path, Files.size(path), sha256(Files.readAllBytes(path))));
                    }
                } catch (IOException ignored) {
                }
            });
        } catch (IOException ignored) {
        }
        models.sort(Comparator.comparing(ModelFile::modelId));
        return List.copyOf(models);
    }

    public ModelFile find(String modelId) {
        return models().stream().filter(model -> model.modelId.equals(modelId)).findFirst().orElse(null);
    }

    public byte[] serverKey() {
        return serverKey.clone();
    }

    public List<CompiledModel> compiledModels() {
        return compiledModels.values().stream()
                .sorted(Comparator.comparing(CompiledModel::modelId))
                .toList();
    }

    public CompiledModel compiled(String modelId) {
        return compiledModels.get(modelId);
    }

    /**
     * Converts Sparkle's serialized model binary into the exact encrypted server-cache
     * container expected by the unmodified Fabric client.
     */
    public synchronized CompiledModel publishCompiled(String modelId, String modelHash, byte[] serializedModel, boolean auth) throws Exception {
        if (normalizeId(modelId) == null) {
            throw new IllegalArgumentException("Invalid compiled model id");
        }
        if (modelHash == null || !modelHash.matches("[0-9a-fA-F]{64}") || serializedModel == null || serializedModel.length == 0) {
            throw new IllegalArgumentException("Invalid compiled model payload");
        }
        String canonicalId = normalizeId(modelId);
        long[] hashes = YsmCrypt.calculateModelHashes(modelHash, serverKey);
        String cacheName = String.format("%016x%016x", hashes[0], hashes[1]);
        Path target = cacheDirectory.resolve(cacheName).normalize();
        if (!target.startsWith(cacheDirectory)) throw new IllegalArgumentException("Cache path escapes storage");
        boolean validCache = false;
        if (Files.isRegularFile(target)) {
            try {
                byte[] existing = Files.readAllBytes(target);
                validCache = YsmCrypt.verifyServerCache(existing, hashes[0], hashes[1]) && YsmCrypt.read(existing, serverKey).length > 0;
            } catch (Exception ignored) { }
        }
        if (!validCache) {
            byte[] cacheData = YsmCrypt.encryptServerCache(serializedModel, serverKey, hashes[0], hashes[1]);
            writeAtomically(target, cacheData);
        }
        CompiledModel compiled = new CompiledModel(canonicalId, modelHash.toLowerCase(Locale.ROOT), hashes[0], hashes[1], target, auth);
        compiledModels.put(canonicalId, compiled);
        saveCatalog();
        return compiled;
    }

    public void setSelection(UUID playerId, String modelId, String textureId) {
        String canonicalId = normalizeId(modelId);
        if (canonicalId == null) return;
        selections.put(playerId, new Selection(canonicalId, textureId == null ? "" : textureId));
        saveSelections();
    }

    public Selection selection(UUID playerId) {
        return selections.getOrDefault(playerId, new Selection("default", ""));
    }

    public void applyMolangState(UUID playerId, int modelHashId, Map<String, Float> values) {
        if (values == null || values.isEmpty()) return;
        molangStates.compute(playerId, (ignored, current) -> {
            Map<String, Float> merged = new java.util.LinkedHashMap<>();
            if (current != null && current.modelHashId() == modelHashId) merged.putAll(current.values());
            merged.putAll(values);
            return new MolangState(modelHashId, Map.copyOf(merged));
        });
        savePlayerStates();
    }

    public MolangState molangState(UUID playerId) {
        return molangStates.get(playerId);
    }

    /** Official scanner checks direct child folders of the custom model root. */
    public List<ModelPack> packs() {
        List<ModelPack> result = new ArrayList<>();
        if (!Files.isDirectory(modelsDirectory)) return List.of();
        try (var folders = Files.list(modelsDirectory)) {
            folders.filter(Files::isDirectory).forEach(folder -> {
                Path packJson = folder.resolve("ysm-pack.json");
                if (!Files.isRegularFile(packJson)) return;
                try {
                    JsonObject json = JsonParser.parseString(Files.readString(packJson)).getAsJsonObject();
                    String name = json.has("name") ? json.get("name").toString() : null;
                    String description = json.has("description") ? json.get("description").toString() : null;
                    Map<String, Map<String, String>> languages = new java.util.HashMap<>();
                    if (json.has("lang") && json.get("lang").isJsonObject()) {
                        for (Map.Entry<String, JsonElement> language : json.getAsJsonObject("lang").entrySet()) {
                            if (!language.getValue().isJsonObject()) continue;
                            Map<String, String> translations = new java.util.HashMap<>();
                            for (Map.Entry<String, JsonElement> entry : language.getValue().getAsJsonObject().entrySet()) {
                                translations.put(entry.getKey(), entry.getValue().toString());
                            }
                            languages.put(language.getKey(), Map.copyOf(translations));
                        }
                    }
                    byte[] icon = null;
                    int width = 0;
                    int height = 0;
                    Path iconFile = folder.resolve("ysm-pack.png");
                    if (Files.isRegularFile(iconFile)) {
                        icon = Files.readAllBytes(iconFile);
                        int[] dimensions = pngDimensions(icon);
                        width = dimensions[0];
                        height = dimensions[1];
                    }
                    result.add(new ModelPack(folder.getFileName().toString() + "/", icon, width, height,
                            name, description, Map.copyOf(languages)));
                } catch (Exception ignored) { }
            });
        } catch (IOException ignored) { }
        result.sort(Comparator.comparing(ModelPack::folderPath));
        return List.copyOf(result);
    }

    public Set<String> updateStar(UUID playerId, String modelId, boolean add) {
        stars.compute(playerId, (ignored, values) -> {
            Set<String> updated = values == null ? new HashSet<>() : new HashSet<>(values);
            if (add) updated.add(modelId); else updated.remove(modelId);
            return Set.copyOf(updated);
        });
        saveStars();
        return stars.getOrDefault(playerId, Set.of());
    }

    public void expireUploads() {
        long now = System.currentTimeMillis();
        uploads.entrySet().removeIf(entry -> now - entry.getValue().lastTouched > timeoutMillis);
    }

    private Path target(String modelId, String extension) {
        Path target = modelsDirectory.resolve(modelId + extension).normalize();
        if (!target.startsWith(modelsDirectory)) throw new IllegalArgumentException("Model path escapes storage");
        return target;
    }

    private static String extension(String fileName) {
        if (fileName == null) return "";
        String lower = fileName.toLowerCase(Locale.ROOT);
        for (String extension : List.of(".ysm", ".zip", ".bbmodel", ".gltf", ".glb")) {
            if (lower.endsWith(extension)) return extension;
        }
        return "";
    }

    private static int[] pngDimensions(byte[] data) {
        if (data == null || data.length < 24 || (data[0] & 0xff) != 0x89 || data[1] != 0x50 || data[2] != 0x4e || data[3] != 0x47) {
            return new int[]{0, 0};
        }
        int width = ((data[16] & 0xff) << 24) | ((data[17] & 0xff) << 16) | ((data[18] & 0xff) << 8) | (data[19] & 0xff);
        int height = ((data[20] & 0xff) << 24) | ((data[21] & 0xff) << 16) | ((data[22] & 0xff) << 8) | (data[23] & 0xff);
        return new int[]{width, height};
    }

    private static String normalizeId(String id) {
        if (id == null) return null;
        String value = id.trim().replace('\\', '/').toLowerCase(Locale.ROOT);
        value = INVALID_MODEL_ID_CHARS.matcher(value)
                .replaceAll("_")
                .replaceAll("/+", "/")
                .replaceAll("^/+", "")
                .replaceAll("/+$", "");
        while (value.contains("..")) value = value.replace("..", ".");
        return !value.isBlank() && MODEL_ID_PATTERN.matcher(value).matches()
                && MODEL_ID_CONTENT_PATTERN.matcher(value).matches() ? value : null;
    }

    private static String migrateLegacyModelId(String value) {
        return value != null && value.startsWith("server/") ? value.substring("server/".length()) : value;
    }

    private static String safeFileName(String modelId, String extension) {
        return modelId + extension;
    }

    private static String sha256(byte[] data) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest) result.append(String.format("%02x", value));
            return result.toString();
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }

    private void loadSelections() {
        YamlConfiguration yaml = YamlConfiguration.loadConfiguration(selectionsFile.toFile());
        for (String key : yaml.getKeys(false)) {
            try {
                UUID playerId = UUID.fromString(key);
                if (yaml.isString(key)) {
                    // v0.2.3 and earlier stored the model id as a scalar.
                    selections.put(playerId, new Selection(migrateLegacyModelId(yaml.getString(key, "default")), ""));
                } else {
                    selections.put(playerId, new Selection(migrateLegacyModelId(yaml.getString(key + ".model", "default")),
                            yaml.getString(key + ".texture", "")));
                }
            } catch (IllegalArgumentException ignored) { }
        }
    }

    private byte[] loadOrCreateServerKey() throws IOException {
        if (Files.exists(serverKeyFile)) {
            byte[] existing = Files.readAllBytes(serverKeyFile);
            if (existing.length == 56) return existing;
            throw new IOException("Invalid Sparkle server key length");
        }
        byte[] created = new byte[56];
        random.nextBytes(created);
        writeAtomically(serverKeyFile, created);
        return created;
    }

    private void loadPlayerStates() {
        YamlConfiguration yaml = YamlConfiguration.loadConfiguration(playerStateFile.toFile());
        for (String key : yaml.getKeys(false)) {
            try {
                UUID playerId = UUID.fromString(key);
                int modelHashId = yaml.getInt(key + ".model-hash");
                Map<String, Float> values = new java.util.LinkedHashMap<>();
                var section = yaml.getConfigurationSection(key + ".values");
                if (section != null) {
                    for (String variable : section.getKeys(false)) {
                        try {
                            String decoded = new String(Base64.getUrlDecoder().decode(variable), java.nio.charset.StandardCharsets.UTF_8);
                            values.put(decoded, (float) section.getDouble(variable));
                        } catch (IllegalArgumentException ignored) { }
                    }
                }
                if (!values.isEmpty()) molangStates.put(playerId, new MolangState(modelHashId, Map.copyOf(values)));
            } catch (IllegalArgumentException ignored) { }
        }
    }

    private void loadCatalog() {
        YamlConfiguration yaml = YamlConfiguration.loadConfiguration(catalogFile.toFile());
        for (String id : yaml.getKeys(false)) {
            String modelHash = yaml.getString(id + ".model-hash");
            long hash1 = yaml.getLong(id + ".hash-1");
            long hash2 = yaml.getLong(id + ".hash-2");
            boolean auth = yaml.getBoolean(id + ".auth", false);
            if (modelHash == null || !modelHash.matches("[0-9a-fA-F]{64}")) continue;
            Path cache = cacheDirectory.resolve(String.format("%016x%016x", hash1, hash2)).normalize();
            if (!cache.startsWith(cacheDirectory) || !Files.isRegularFile(cache)) continue;
            compiledModels.put(id, new CompiledModel(id, modelHash, hash1, hash2, cache, auth));
        }
    }

    private void loadStars() {
        YamlConfiguration yaml = YamlConfiguration.loadConfiguration(starsFile.toFile());
        for (String key : yaml.getKeys(false)) {
            try { stars.put(UUID.fromString(key), Set.copyOf(yaml.getStringList(key))); } catch (IllegalArgumentException ignored) { }
        }
    }

    private synchronized void saveCatalog() {
        YamlConfiguration yaml = new YamlConfiguration();
        compiledModels.forEach((id, model) -> {
            yaml.set(id + ".model-hash", model.modelHash());
            yaml.set(id + ".hash-1", model.hash1());
            yaml.set(id + ".hash-2", model.hash2());
            yaml.set(id + ".auth", model.auth());
        });
        try { yaml.save(catalogFile.toFile()); } catch (IOException ignored) { }
    }

    /** Clears only regenerated cache and catalog data before a full source scan. */
    public synchronized void resetDerivedCatalog() throws IOException {
        compiledModels.clear();
        if (Files.isDirectory(cacheDirectory)) {
            try (var files = Files.list(cacheDirectory)) {
                for (Path file : files.toList()) Files.deleteIfExists(file);
            }
        }
        Files.deleteIfExists(catalogFile);
    }

    private static void writeAtomically(Path target, byte[] data) throws IOException {
        Files.createDirectories(target.getParent());
        Path temporary = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
        try {
            Files.write(temporary, data);
            try {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException ignored) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private synchronized void saveSelections() {
        YamlConfiguration yaml = new YamlConfiguration();
        selections.forEach((id, selection) -> {
            yaml.set(id + ".model", selection.modelId());
            yaml.set(id + ".texture", selection.textureId());
        });
        try { yaml.save(selectionsFile.toFile()); } catch (IOException ignored) { }
    }

    private synchronized void savePlayerStates() {
        YamlConfiguration yaml = new YamlConfiguration();
        molangStates.forEach((id, state) -> {
            yaml.set(id + ".model-hash", state.modelHashId());
            state.values().forEach((variable, value) -> {
                String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(variable.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                yaml.set(id + ".values." + encoded, value);
            });
        });
        try { yaml.save(playerStateFile.toFile()); } catch (IOException ignored) { }
    }

    private synchronized void saveStars() {
        YamlConfiguration yaml = new YamlConfiguration();
        stars.forEach((id, values) -> yaml.set(id.toString(), List.copyOf(values)));
        try { yaml.save(starsFile.toFile()); } catch (IOException ignored) { }
    }
}
