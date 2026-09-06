package local.nanoda.sparklemorpher.storage;

import java.nio.file.Path;

/** Offline batch compiler used to prime an existing server model directory. */
public final class BatchCompilerMain {
    private BatchCompilerMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expected the SparkleMorpherBridge data directory");
        }
        ModelStore store = new ModelStore(Path.of(args[0]), 64 * 1024 * 1024, 300);
        store.resetDerivedCatalog();
        ModelCompiler compiler = new ModelCompiler(store);
        int compiled = 0;
        int failed = 0;
        for (ModelStore.ModelFile model : store.models()) {
            try {
                ModelStore.CompiledModel result = compiler.compile(model);
                compiled++;
                System.out.println("OK " + result.modelId() + " " + result.cacheFile().getFileName());
            } catch (Exception error) {
                failed++;
                System.err.println("FAIL " + model.fileName() + ": " + error.getMessage());
            }
        }
        System.out.println("DONE compiled=" + compiled + " failed=" + failed);
        if (failed > 0) System.exit(2);
    }
}
