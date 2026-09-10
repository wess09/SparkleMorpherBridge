package local.nanoda.sparklemorpher.storage;

import com.micaftic.morpher.core.security.YsmCrypt;
import com.micaftic.morpher.resource.YSMFolderDeserializer;
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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.Base64;

/** Persistent server model storage plus bounded, ordered upload sessions. */
public final class ModelStore {
    private static final Pattern INVALID_MODEL_ID_CHARS = Pattern.compile("[^\\p{L}\\p{M}\\p{N}_./-]+");
    private static final Pattern MODEL_ID_PATTERN = Pattern.compile("[\\p{L}\\p{M}\\p{N}_./-]+");
    /** Max pack icon bytes read from disk; anything larger cannot fit one plugin message anyway. */
    private static final long MAX_ICON_BYTES = 1_048_000L;
    /** Bound the Molang snapshot per (player, model) so a state frame can never overflow. */
    private static final int MOLANG_VARS_MAX = 4096;
    /** Bound one player's star set; far beyond real use yet always fits a single frame. */
    private static final int MAX_STARS = 4096;
    private static final Pattern MODEL_ID_CONTENT_PATTERN = Pattern.compile(".*[\\p{L}\\p{N}].*");
    public record ModelFile(String modelId, String fileName, Path path, long size, String sha256) {
    }

    public record UploadStart(long id, byte status, String message) {
    }

    public record UploadResult(long id, byte status, String modelId, String message) {
    }

    /**
     * Metadata for a model already transformed into Sparkle's server-cache format.
     * {@code sourceSha256} is the raw source file's hash at compile time, used to
     * skip re-decoding unchanged models on later boots.
     */
    public record CompiledModel(String modelId, String modelHash, long hash1, long hash2, Path cacheFile,
                                boolean auth, String sourceSha256) {
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

    /**
     * In-flight upload. The buffer is allocated lazily and only grows to the bytes
     * actually received (chunks arrive strictly contiguous), so a client that begins
     * an upload and never fills it reserves no memory — matching the official
     * ModelUploadSession. {@code totalBytes} is the declared final size; {@code data}
     * always holds exactly {@code received} bytes.
     */
    private record Upload(UUID owner, String requestedId, String modelId, String fileName, String sha256,
                          int totalBytes, byte[] data, long lastTouched, int received) {
        Upload touch() {
            return new Upload(owner, requestedId, modelId, fileName, sha256, totalBytes, data, System.currentTimeMillis(), received);
        }
        Upload append(int offset, byte[] chunk) {
            if (chunk == null || offset != received || offset < 0 || received + chunk.length > totalBytes) return null;
            int end = received + chunk.length;
            byte[] grown = data.length >= end ? data : java.util.Arrays.copyOf(data, end);
            System.arraycopy(chunk, 0, grown, offset, chunk.length);
            return new Upload(owner, requestedId, modelId, fileName, sha256, totalBytes, grown, System.currentTimeMillis(), end);
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
    /** Raw source-file hashes whose compilation failed; retried only if the file changes. */
    private final Map<String, String> failedSources = new ConcurrentHashMap<>();
    private final SecureRandom random = new SecureRandom();
    private final int maxBytes;
    private final long timeoutMillis;
    private final byte[] serverKey;
    /**
     * Dedicated single-thread executor for persisting per-player YAML (selections /
     * stars / molang state). The store mutators run on Folia region threads and must
     * never block on disk I/O; they only mark the file dirty and the writes happen
     * here, coalesced to at most one in-flight snapshot per file.
     */
    private final Executor persistExecutor;
    private final AtomicBoolean selectionsPending = new AtomicBoolean();
    private final AtomicBoolean playerStatesPending = new AtomicBoolean();
    private final AtomicBoolean starsPending = new AtomicBoolean();
    /** Hard cap on concurrent in-flight upload sessions per player. */
    private static final int MAX_UPLOADS_PER_PLAYER = 4;

    public ModelStore(Path dataDirectory, int maxBytes, int timeoutSeconds, Executor persistExecutor) throws IOException {
        this.modelsDirectory = dataDirectory.resolve("models").toAbsolutePath().normalize();
        this.cacheDirectory = dataDirectory.resolve("server-cache").toAbsolutePath().normalize();
        this.selectionsFile = dataDirectory.resolve("selections.yml");
        this.playerStateFile = dataDirectory.resolve("player-state.yml");
        this.starsFile = dataDirectory.resolve("stars.yml");
        this.catalogFile = dataDirectory.resolve("catalog.yml");
        this.serverKeyFile = dataDirectory.resolve("server-key.bin");
        this.maxBytes = maxBytes;
        this.timeoutMillis = timeoutSeconds * 1000L;
        this.persistExecutor = persistExecutor;
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
        long activeForOwner = uploads.values().stream().filter(upload -> upload.owner.equals(owner)).count();
        if (activeForOwner >= MAX_UPLOADS_PER_PLAYER) {
            return new UploadStart(0, (byte) 2, "Too many concurrent uploads");
        }
        long id;
        do { id = random.nextLong(); } while (id == 0 || uploads.containsKey(id));
        uploads.put(id, new Upload(owner, localId, modelId, safeFileName(localId, extension), sha256.toLowerCase(Locale.ROOT), totalBytes, new byte[0], System.currentTimeMillis(), 0));
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
        if (upload.received != upload.totalBytes) return new UploadResult(uploadId, (byte) 5, "", "Incomplete upload");
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
        // Two intake shapes, mirroring what a real YSM server catalog accepts:
        //  1. single-file releases (.ysm/.zip/.bbmodel/.gltf/.glb) anywhere
        //     under the root;
        //  2. the standard YSM folder library: a directory that carries a model
        //     (ysm.json, or main.json + arm.json), as found inside model packs
        //     next to their ysm-pack.json. Such folders are compiled by
        //     YSMFolderDeserializer exactly like the mod server's
        //     LocalModelScanner intake.
        // A directory that already is a model folder is treated as opaque: its
        // subtree is not scanned further. Otherwise the inner "models/"
        // subfolder of a ysm.json-format pack (which contains main.json/arm.json)
        // would be mis-detected as a second, contextless model folder and fail.
        try {
            Files.walkFileTree(modelsDirectory, new java.nio.file.SimpleFileVisitor<>() {
                @Override
                public java.nio.file.FileVisitResult preVisitDirectory(Path dir, java.nio.file.attribute.BasicFileAttributes attrs) {
                    if (dir.equals(modelsDirectory)) {
                        return java.nio.file.FileVisitResult.CONTINUE;
                    }
                    String relative = modelsDirectory.relativize(dir).toString().replace('\\', '/');
                    if (YSMFolderDeserializer.isModelFolder(dir)) {
                        String id = normalizeId(relative);
                        if (id != null) {
                            models.add(new ModelFile(id, relative, dir, 0L, sha256Folder(dir)));
                        }
                        return java.nio.file.FileVisitResult.SKIP_SUBTREE;
                    }
                    return java.nio.file.FileVisitResult.CONTINUE;
                }

                @Override
                public java.nio.file.FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs) {
                    try {
                        String relative = modelsDirectory.relativize(file).toString().replace('\\', '/');
                        String extension = extension(relative);
                        if (extension.isEmpty()) return java.nio.file.FileVisitResult.CONTINUE;
                        String baseId = relative.substring(0, relative.length() - extension.length());
                        String id = normalizeId(baseId);
                        if (id != null) {
                            models.add(new ModelFile(id, relative, file, Files.size(file), sha256(file)));
                        }
                    } catch (IOException ignored) {
                    }
                    return java.nio.file.FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ignored) {
        }
        models.sort(Comparator.comparing(ModelFile::modelId));
        return List.copyOf(models);
    }

    /** Stable content hash over a model folder (sorted file bytes), used as its cache identity. */
    private static String sha256Folder(Path folder) {
        try {
            List<Path> files;
            try (var walk = Files.walk(folder)) {
                files = walk.filter(Files::isRegularFile).sorted().toList();
            }
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (Path file : files) {
                try (java.io.InputStream in = Files.newInputStream(file)) {
                    byte[] buffer = new byte[8192];
                    int read;
                    while ((read = in.read(buffer)) >= 0) {
                        if (read > 0) digest.update(buffer, 0, read);
                    }
                }
            }
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest.digest()) result.append(String.format("%02x", value));
            return result.toString();
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }

    /** Streaming SHA-256 of a single file (avoids buffering the whole source for hashing). */
    private static String sha256(Path file) {
        try (java.io.InputStream in = Files.newInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) >= 0) {
                if (read > 0) digest.update(buffer, 0, read);
            }
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest.digest()) result.append(String.format("%02x", value));
            return result.toString();
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
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
    public synchronized CompiledModel publishCompiled(String modelId, String modelHash, byte[] serializedModel, boolean auth, String sourceSha256) throws Exception {
        if (normalizeId(modelId) == null) {
            throw new IllegalArgumentException("Invalid compiled model id");
        }
        if (modelHash == null || !modelHash.matches("[0-9a-fA-F]{64}") || serializedModel == null || serializedModel.length == 0) {
            throw new IllegalArgumentException("Invalid compiled model payload");
        }
        String canonicalId = normalizeId(modelId);
        long[] hashes = YsmCrypt.calculateModelHashes(modelHash, serverKey);
        String cacheName = cacheName(hashes[0], hashes[1]);
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
        CompiledModel compiled = new CompiledModel(canonicalId, modelHash.toLowerCase(Locale.ROOT), hashes[0], hashes[1],
                target, auth, sourceSha256);
        compiledModels.put(canonicalId, compiled);
        failedSources.remove(canonicalId);
        saveCatalog();
        return compiled;
    }

    /**
     * True when the model is already compiled and the raw source bytes are
     * unchanged since, so the boot scan can skip re-decoding it. Catalog entries
     * written by older versions carry no {@code source-sha}; for those the file
     * hash matching the recorded model hash still proves the file is untouched
     * for legacy packages whose container hash doubles as their identity.
     */
    public boolean isUpToDate(String modelId, String sourceSha256) {
        if (sourceSha256 == null) return false;
        CompiledModel compiled = compiledModels.get(modelId);
        if (compiled == null) return false;
        if (sourceSha256.equals(compiled.sourceSha256())) return true;
        return compiled.sourceSha256() == null && sourceSha256.equals(compiled.modelHash());
    }

    /** True when this exact source file already failed to compile on a prior boot. */
    public boolean isKnownBad(String modelId, String sourceSha256) {
        return sourceSha256 != null && sourceSha256.equals(failedSources.get(modelId));
    }

    public synchronized void clearFailure(String modelId) {
        if (failedSources.remove(modelId) != null) saveCatalog();
    }

    public synchronized void rememberFailure(String modelId, String sourceSha256) {
        if (sourceSha256 == null) return;
        failedSources.put(modelId, sourceSha256);
        saveCatalog();
    }

    public void setSelection(UUID playerId, String modelId, String textureId) {
        String canonicalId = normalizeId(modelId);
        if (canonicalId == null) return;
        selections.put(playerId, new Selection(canonicalId, textureId == null ? "" : textureId));
        scheduleSaveSelections();
    }

    public Selection selection(UUID playerId) {
        return selections.getOrDefault(playerId, new Selection("default", ""));
    }

    /** The player's persisted star set (mirrors the official S2CSyncStarModelsPacket content). */
    public Set<String> stars(UUID playerId) {
        return stars.getOrDefault(playerId, Set.of());
    }

    public void applyMolangState(UUID playerId, int modelHashId, Map<String, Float> values) {
        if (values == null || values.isEmpty()) return;
        molangStates.compute(playerId, (ignored, current) -> {
            // Newest values first, then the older keys not overwritten; the map is
            // capped so a single state frame can never grow past the plugin-message
            // limit (and player-state.yml cannot grow without bound).
            Map<String, Float> merged = new java.util.LinkedHashMap<>();
            merged.putAll(values);
            if (current != null && current.modelHashId() == modelHashId) {
                for (Map.Entry<String, Float> entry : current.values().entrySet()) {
                    if (!merged.containsKey(entry.getKey())) merged.put(entry.getKey(), entry.getValue());
                }
            }
            Map<String, Float> capped = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, Float> entry : merged.entrySet()) {
                if (capped.size() >= MOLANG_VARS_MAX) break;
                capped.put(entry.getKey(), entry.getValue());
            }
            return new MolangState(modelHashId, Map.copyOf(capped));
        });
        scheduleSavePlayerStates();
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
                    // getAsString (not toString): toString() keeps the JSON quotes,
                    // which the client then displays literally.
                    String name = json.has("name") ? json.get("name").getAsString() : null;
                    String description = json.has("description") ? json.get("description").getAsString() : null;
                    Map<String, Map<String, String>> languages = new java.util.HashMap<>();
                    if (json.has("lang") && json.get("lang").isJsonObject()) {
                        for (Map.Entry<String, JsonElement> language : json.getAsJsonObject("lang").entrySet()) {
                            if (!language.getValue().isJsonObject()) continue;
                            Map<String, String> translations = new java.util.HashMap<>();
                            for (Map.Entry<String, JsonElement> entry : language.getValue().getAsJsonObject().entrySet()) {
                                translations.put(entry.getKey(), entry.getValue().getAsString());
                            }
                            languages.put(language.getKey(), Map.copyOf(translations));
                        }
                    }
                    byte[] icon = null;
                    int width = 0;
                    int height = 0;
                    Path iconFile = folder.resolve("ysm-pack.png");
                    if (Files.isRegularFile(iconFile) && Files.size(iconFile) <= MAX_ICON_BYTES) {
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
            if (add) {
                if (updated.size() < MAX_STARS) updated.add(modelId);
            } else {
                updated.remove(modelId);
            }
            return Set.copyOf(updated);
        });
        scheduleSaveStars();
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
                if (!values.isEmpty()) {
                    // Trim any oversized snapshot persisted by an older build so a
                    // reloaded state frame can never exceed the message limit.
                    Map<String, Float> capped = new java.util.LinkedHashMap<>();
                    for (Map.Entry<String, Float> entry : values.entrySet()) {
                        if (capped.size() >= MOLANG_VARS_MAX) break;
                        capped.put(entry.getKey(), entry.getValue());
                    }
                    if (!capped.isEmpty()) molangStates.put(playerId, new MolangState(modelHashId, Map.copyOf(capped)));
                }
            } catch (IllegalArgumentException ignored) { }
        }
    }

    private void loadCatalog() {
        YamlConfiguration yaml = YamlConfiguration.loadConfiguration(catalogFile.toFile());
        for (String id : yaml.getKeys(false)) {
            // Compile failures are intentionally never persisted: catalog.yml holds
            // only successfully compiled models. Each boot retries a failing source
            // once (and logs one warning per boot for it).
            String modelHash = yaml.getString(id + ".model-hash");
            if (modelHash == null || !modelHash.matches("[0-9a-fA-F]{64}")) continue;
            long hash1 = yaml.getLong(id + ".hash-1");
            long hash2 = yaml.getLong(id + ".hash-2");
            boolean auth = yaml.getBoolean(id + ".auth", false);
            String sourceSha = yaml.getString(id + ".source-sha");
            Path cache = cacheDirectory.resolve(cacheName(hash1, hash2)).normalize();
            if (!cache.startsWith(cacheDirectory) || !Files.isRegularFile(cache)) continue;
            compiledModels.put(id, new CompiledModel(id, modelHash, hash1, hash2, cache, auth,
                    sourceSha == null || sourceSha.isEmpty() ? null : sourceSha));
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
            if (model.sourceSha256() != null) yaml.set(id + ".source-sha", model.sourceSha256());
        });
        // Failures are kept in memory only (per boot) and are never written out.
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

    private void scheduleSaveSelections() { scheduleSave(selectionsPending, this::writeSelections); }
    private void scheduleSavePlayerStates() { scheduleSave(playerStatesPending, this::writePlayerStates); }
    private void scheduleSaveStars() { scheduleSave(starsPending, this::writeStars); }

    /**
     * Queues a coalesced write of {@code write} onto the persist executor. At most
     * one snapshot per file is in flight: further mutations while one is pending
     * only update the in-memory maps, which the pending write reads at execution
     * time, so the newest state is persisted without a per-mutation disk write.
     */
    private void scheduleSave(AtomicBoolean pending, Runnable write) {
        if (!pending.compareAndSet(false, true)) {
            return; // a snapshot write is already queued/running; it will pick up this change
        }
        try {
            persistExecutor.execute(() -> {
                try {
                    write.run();
                } catch (Throwable ignored) {
                    // A disk failure must not take down the single persist thread.
                } finally {
                    pending.set(false);
                }
            });
        } catch (RuntimeException rejected) {
            // Executor already shut down (plugin disabling); drop persistence.
            pending.set(false);
        }
    }

    // The write methods are synchronized on the store so the single persist worker
    // and a disable-time flush can never write the same YAML concurrently. Region
    // threads never take this monitor — they only schedule through scheduleSave.
    private synchronized void writeSelections() {
        YamlConfiguration yaml = new YamlConfiguration();
        selections.forEach((id, selection) -> {
            yaml.set(id + ".model", selection.modelId());
            yaml.set(id + ".texture", selection.textureId());
        });
        try { yaml.save(selectionsFile.toFile()); } catch (IOException ignored) { }
    }

    private synchronized void writePlayerStates() {
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

    private synchronized void writeStars() {
        YamlConfiguration yaml = new YamlConfiguration();
        stars.forEach((id, values) -> yaml.set(id.toString(), List.copyOf(values)));
        try { yaml.save(starsFile.toFile()); } catch (IOException ignored) { }
    }

    // ------------------------------------------------------------------
    // Admin / diagnostics surface. Read-only projections are safe on any
    // thread; the destructive methods touch disk and MUST only run on the
    // plugin's storage worker (never a Folia region thread).
    // ------------------------------------------------------------------

    /** Snapshot of one in-flight upload for the /spm uploads diagnostic. */
    public record UploadInfo(long id, UUID owner, String modelId, String fileName,
                             int receivedBytes, int totalBytes, long idleMillis, boolean expired) { }

    /** server-cache/ census. orphans = files referenced by no CompiledModel. */
    public record CacheStats(int files, long totalBytes, int orphans, long orphanBytes, int missingForCatalog) { }

    /** Aggregate storage census for /spm stats. */
    public record StorageStats(int sourceFiles, long sourceBytes, int compiledModels, long cacheBytes,
                               int packs, int selections, int starSets, int molangStates) { }

    /** Per-model cache verification. status: 0 ok, 1 corrupt, 2 unreadable, 3 missing, 4 decrypt-failed. */
    public record CacheVerification(String modelId, byte status, long bytes, String detail) { }

    /** Outcome of deleteModel(). */
    public record DeleteResult(boolean sourceDeleted, boolean cacheDeleted, boolean catalogRowRemoved,
                               int selectionsReset, String message) { }

    /** Outcome of pruneCache(). */
    public record PruneResult(int orphansDeleted, long bytesReclaimed, int catalogRowsDropped) { }

    /** Cache file name derived from the model hash pair; the client requests exactly this. */
    private static String cacheName(long hash1, long hash2) {
        return String.format("%016x%016x", hash1, hash2);
    }

    /** Snapshot of in-flight uploads, sorted by id. Read-only: does NOT expire sessions. */
    public List<UploadInfo> activeUploads() {
        long now = System.currentTimeMillis();
        List<UploadInfo> out = new ArrayList<>();
        uploads.forEach((id, upload) -> out.add(new UploadInfo(id, upload.owner, upload.modelId, upload.fileName,
                upload.received, upload.totalBytes, now - upload.lastTouched, now - upload.lastTouched > timeoutMillis)));
        out.sort(Comparator.comparingLong(UploadInfo::id));
        return List.copyOf(out);
    }

    /** Census of server-cache/ plus catalog entries whose cache file is missing. */
    public CacheStats cacheStats() {
        Set<Path> referenced = referencedCachePaths();
        int files = 0, orphans = 0;
        long totalBytes = 0L, orphanBytes = 0L;
        if (Files.isDirectory(cacheDirectory)) {
            try (var list = Files.list(cacheDirectory)) {
                for (Path file : list.toList()) {
                    if (!Files.isRegularFile(file)) continue;
                    files++;
                    long size;
                    try { size = Files.size(file); } catch (IOException e) { size = 0L; }
                    totalBytes += size;
                    if (!referenced.contains(file.normalize())) { orphans++; orphanBytes += size; }
                }
            } catch (IOException ignored) { }
        }
        int missing = 0;
        for (CompiledModel model : compiledModels.values()) {
            if (!Files.isRegularFile(model.cacheFile())) missing++;
        }
        return new CacheStats(files, totalBytes, orphans, orphanBytes, missing);
    }

    /** Aggregate census across models/, server-cache/ and the in-memory player maps. */
    public StorageStats storageStats() {
        List<ModelFile> modelFiles = models();
        long sourceBytes = 0L;
        int sourceFiles = 0;
        for (ModelFile model : modelFiles) {
            if (Files.isDirectory(model.path())) continue; // folder models counted but have no single-file size
            sourceFiles++;
            sourceBytes += model.size();
        }
        return new StorageStats(sourceFiles, sourceBytes, compiledModels.size(), cacheStats().totalBytes(),
                packs().size(), selections.size(), stars.size(), molangStates.size());
    }

    private Set<Path> referencedCachePaths() {
        Set<Path> out = new HashSet<>();
        for (CompiledModel model : compiledModels.values()) out.add(model.cacheFile().normalize());
        return out;
    }

    /**
     * Verifies one compiled model's cache file, or every one when {@code modelId} is
     * null. Re-derives the CityHash signature and decrypts with the server key, so a
     * corrupted or mis-keyed container is caught. Reads whole files: storage worker only.
     */
    public List<CacheVerification> verifyCache(String modelId) {
        List<CacheVerification> out = new ArrayList<>();
        if (modelId != null) {
            String id = normalizeId(modelId);
            CompiledModel model = compiledModels.get(id == null ? modelId : id);
            if (model == null) { out.add(new CacheVerification(modelId, (byte) 3, 0L, "model not compiled")); return List.copyOf(out); }
            out.add(verifyOne(model));
            return List.copyOf(out);
        }
        for (CompiledModel model : compiledModels()) out.add(verifyOne(model));
        return List.copyOf(out);
    }

    private CacheVerification verifyOne(CompiledModel model) {
        Path file = model.cacheFile();
        if (!Files.isRegularFile(file)) return new CacheVerification(model.modelId(), (byte) 3, 0L, "cache file missing");
        byte[] data;
        try { data = Files.readAllBytes(file); }
        catch (IOException e) { return new CacheVerification(model.modelId(), (byte) 2, 0L, String.valueOf(e.getMessage())); }
        if (!YsmCrypt.verifyServerCache(data, model.hash1(), model.hash2())) {
            return new CacheVerification(model.modelId(), (byte) 1, data.length, "signature mismatch");
        }
        try {
            byte[] clear = YsmCrypt.read(data, serverKey);
            if (clear == null || clear.length == 0) return new CacheVerification(model.modelId(), (byte) 4, data.length, "empty payload");
        } catch (Throwable t) {
            return new CacheVerification(model.modelId(), (byte) 4, data.length, String.valueOf(t.getMessage()));
        }
        return new CacheVerification(model.modelId(), (byte) 0, data.length, "ok");
    }

    /**
     * Removes one model: always drops the catalog row and (when unshared) its cache
     * file; deletes the source under models/ only when {@code deleteSource} is true;
     * resets every player selection that pointed at it. Storage worker only.
     */
    public synchronized DeleteResult deleteModel(String modelId, boolean deleteSource) throws IOException {
        String id = normalizeId(modelId);
        if (id == null) return new DeleteResult(false, false, false, 0, "invalid model id");
        CompiledModel model = compiledModels.get(id);
        boolean catalogRemoved = compiledModels.remove(id) != null;
        failedSources.remove(id);
        boolean cacheDeleted = false;
        if (model != null) {
            Path cache = model.cacheFile().normalize();
            if (cache.startsWith(cacheDirectory) && !cachePathShared(cache, id) && Files.isRegularFile(cache)) {
                Files.deleteIfExists(cache);
                cacheDeleted = true;
            }
        }
        boolean sourceDeleted = false;
        if (deleteSource) {
            ModelFile source = models().stream().filter(m -> m.modelId().equals(id)).findFirst().orElse(null);
            if (source != null) { deleteSourceRecursively(source.path()); sourceDeleted = true; }
        }
        int reset = resetSelectionsFor(id);
        saveCatalog();
        if (!catalogRemoved && !sourceDeleted) return new DeleteResult(false, cacheDeleted, false, reset, "model not found");
        return new DeleteResult(sourceDeleted, cacheDeleted, catalogRemoved, reset, "");
    }

    /** True when another compiled model resolves to the same cache file path. */
    private boolean cachePathShared(Path cache, String exceptModelId) {
        for (Map.Entry<String, CompiledModel> entry : compiledModels.entrySet()) {
            if (!entry.getKey().equals(exceptModelId) && entry.getValue().cacheFile().normalize().equals(cache)) return true;
        }
        return false;
    }

    /**
     * Drops only the derived state for one model (catalog row + unshared cache file);
     * the source under models/ is left untouched so a later compile re-derives it.
     * Returns the cache path removed, or null. Storage worker only.
     */
    public synchronized Path invalidateCompiled(String modelId) throws IOException {
        String id = normalizeId(modelId);
        if (id == null) return null;
        CompiledModel model = compiledModels.get(id);
        if (model == null) return null;
        failedSources.remove(id);
        Path cache = model.cacheFile().normalize();
        Path removed = null;
        if (cache.startsWith(cacheDirectory) && !cachePathShared(cache, id) && Files.isRegularFile(cache)) {
            Files.deleteIfExists(cache);
            removed = cache;
        }
        compiledModels.remove(id);
        saveCatalog();
        return removed;
    }

    /** Deletes server-cache/ files no compiled model references and drops catalog rows whose cache file is gone. */
    public synchronized PruneResult pruneCache() throws IOException {
        Set<Path> referenced = referencedCachePaths();
        int deleted = 0;
        long reclaimed = 0L;
        if (Files.isDirectory(cacheDirectory)) {
            try (var list = Files.list(cacheDirectory)) {
                for (Path file : list.toList()) {
                    if (!Files.isRegularFile(file) || referenced.contains(file.normalize())) continue;
                    long size;
                    try { size = Files.size(file); } catch (IOException e) { size = 0L; }
                    if (Files.deleteIfExists(file)) { deleted++; reclaimed += size; }
                }
            }
        }
        int dropped = 0;
        for (String id : new ArrayList<>(compiledModels.keySet())) {
            CompiledModel model = compiledModels.get(id);
            if (model != null && !Files.isRegularFile(model.cacheFile())) { compiledModels.remove(id); dropped++; }
        }
        if (dropped > 0) saveCatalog();
        return new PruneResult(deleted, reclaimed, dropped);
    }

    /** Clears selection, stars and Molang state for one player. Returns the prior selection id. */
    public String clearPlayerState(UUID playerId) {
        if (playerId == null) return "default";
        Selection prior = selections.getOrDefault(playerId, new Selection("default", ""));
        selections.put(playerId, new Selection("default", ""));
        molangStates.remove(playerId);
        stars.remove(playerId);
        scheduleSaveSelections();
        scheduleSavePlayerStates();
        scheduleSaveStars();
        return prior.modelId();
    }

    /** Resets every stored selection that currently points at {@code modelId}; returns how many changed. */
    public int resetSelectionsFor(String modelId) {
        if (modelId == null) return 0;
        int changed = 0;
        for (Map.Entry<UUID, Selection> entry : selections.entrySet()) {
            if (modelId.equals(entry.getValue().modelId())) {
                entry.setValue(new Selection("default", ""));
                changed++;
            }
        }
        if (changed > 0) scheduleSaveSelections();
        return changed;
    }

    /** Deletes a source file or folder under models/; refuses anything outside the models directory. */
    private void deleteSourceRecursively(Path path) throws IOException {
        Path normalized = path.toAbsolutePath().normalize();
        if (!normalized.startsWith(modelsDirectory) || !Files.exists(normalized)) return;
        if (Files.isDirectory(normalized)) {
            List<Path> paths;
            try (var walk = Files.walk(normalized)) { paths = walk.sorted(Comparator.reverseOrder()).toList(); }
            for (Path p : paths) Files.deleteIfExists(p);
        } else {
            Files.deleteIfExists(normalized);
        }
    }

    /** Runs any pending per-player snapshot write synchronously (used on disable). */
    public void flushPlayerStateSaves() {
        writeSelections();
        writeStars();
        writePlayerStates();
    }
}
