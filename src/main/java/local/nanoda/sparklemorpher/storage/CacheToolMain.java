package local.nanoda.sparklemorpher.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Offline harness for the admin/cache tooling. Runs the same ModelStore code the
 * plugin uses, but with an inline persist executor (no Bukkit, no Folia).
 *
 * <pre>
 *   java ... CacheToolMain &lt;pluginDataFolder&gt; &lt;verify|prune|delete|rebuild|info&gt; [modelId]
 * </pre>
 */
public final class CacheToolMain {

    private CacheToolMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: CacheToolMain <dataDir> <verify|prune|delete|rebuild|info> [modelId]");
        }
        ModelStore store = new ModelStore(Path.of(args[0]), 64 * 1024 * 1024, 300, Runnable::run);
        String command = args[1].toLowerCase(java.util.Locale.ROOT);
        String modelId = args.length >= 3 ? args[2] : null;
        ModelCompiler compiler = new ModelCompiler(store);

        switch (command) {
            case "info" -> printInfo(store);
            case "verify" -> {
                List<ModelStore.CacheVerification> results = store.verifyCache(modelId);
                int ok = 0;
                for (ModelStore.CacheVerification v : results) {
                    System.out.println((v.status() == 0 ? "OK   " : "FAIL ") + v.modelId()
                            + " status=" + v.status() + " bytes=" + v.bytes() + " " + v.detail());
                    if (v.status() == 0) ok++;
                }
                System.out.println("VERIFY total=" + results.size() + " ok=" + ok + " fail=" + (results.size() - ok));
            }
            case "prune" -> {
                ModelStore.PruneResult r = store.pruneCache();
                System.out.println("PRUNE orphansDeleted=" + r.orphansDeleted() + " bytesReclaimed="
                        + r.bytesReclaimed() + " catalogRowsDropped=" + r.catalogRowsDropped());
            }
            case "delete" -> {
                if (modelId == null) throw new IllegalArgumentException("delete requires a modelId");
                ModelStore.DeleteResult r = store.deleteModel(modelId, false);
                System.out.println("DELETE catalog=" + r.catalogRowRemoved() + " cache=" + r.cacheDeleted()
                        + " source=" + r.sourceDeleted() + " selectionsReset=" + r.selectionsReset()
                        + " msg=" + r.message());
            }
            case "rebuild" -> {
                if (modelId == null) throw new IllegalArgumentException("rebuild requires a modelId");
                Path removed = store.invalidateCompiled(modelId);
                ModelStore.ModelFile source = store.find(modelId);
                if (source == null) { System.out.println("REBUILD source-missing " + modelId); break; }
                ModelStore.CompiledModel compiled = compiler.compile(source);
                System.out.println("REBUILD removedCache=" + removed + " -> newCache=" + compiled.cacheFile()
                        + " verify=" + (store.verifyCache(modelId).get(0).status() == 0));
            }
            default -> throw new IllegalArgumentException("Unknown command " + command);
        }
    }

    private static void printInfo(ModelStore store) {
        ModelStore.CacheStats c = store.cacheStats();
        ModelStore.StorageStats s = store.storageStats();
        System.out.println("INFO compiled=" + s.compiledModels() + " sourceFiles=" + s.sourceFiles()
                + " packs=" + s.packs() + " selections=" + s.selections());
        System.out.println("INFO cache files=" + c.files() + " bytes=" + c.totalBytes()
                + " orphans=" + c.orphans() + " orphanBytes=" + c.orphanBytes()
                + " missingForCatalog=" + c.missingForCatalog());
    }
}
