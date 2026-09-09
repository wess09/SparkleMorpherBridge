package local.nanoda.sparklemorpher;

import local.nanoda.sparklemorpher.protocol.SparkleWire;
import local.nanoda.sparklemorpher.protocol.LegacySyncService;
import local.nanoda.sparklemorpher.storage.ModelStore;
import local.nanoda.sparklemorpher.storage.ModelCompiler;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.PluginCommand;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.event.player.PlayerRegisterChannelEvent;
import io.papermc.paper.event.player.PlayerTrackEntityEvent;
import io.papermc.paper.event.player.PlayerUntrackEntityEvent;
import io.papermc.paper.threadedregions.scheduler.ScheduledTask;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.plugin.messaging.PluginMessageListener;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/** Folia-safe protocol bridge for Sparkle Morpher Fabric 26.1.2 clients. */
public final class SparkleMorpherBridgePlugin extends JavaPlugin implements Listener, PluginMessageListener, CommandExecutor {
    private final Map<UUID, String> negotiatedClients = new ConcurrentHashMap<>();
    private final Map<UUID, ConcurrentLinkedQueue<byte[]>> outboundPayloads = new ConcurrentHashMap<>();
    private final Map<UUID, Boolean> outboundDrains = new ConcurrentHashMap<>();
    /** Bytes of payload queued for a player that have not reached the network yet. */
    private final Map<UUID, AtomicLong> outboundPending = new ConcurrentHashMap<>();
    private final Map<UUID, Set<UUID>> trackedBy = new ConcurrentHashMap<>();
    /**
     * Last entity-state values broadcast per player for the S2C-21 delta sync. Seeded
     * at handshake (startStateMonitor); a change against these values is broadcast to
     * tracking players so remote models follow flight/hunger state.
     */
    private final Map<UUID, Boolean> knownFlying = new ConcurrentHashMap<>();
    private final Map<UUID, Integer> knownFood = new ConcurrentHashMap<>();
    private final Map<UUID, ScheduledTask> stateMonitors = new ConcurrentHashMap<>();
    private volatile boolean allowModelUpload;
    private volatile int uploadChunkBytes;
    private volatile int uploadChunksPerTick;
    private volatile int downloadChunksPerTick;
    private volatile long downloadBacklogBytes;
    /** Set once the boot catalog scan completes; model sync waits on it (see LegacySyncService). */
    private volatile boolean catalogReady;
    private ModelStore modelStore;
    private ModelCompiler modelCompiler;
    private LegacySyncService legacySync;
    private ExecutorService storageWorker;
    private ExecutorService downloadWorker;
    private ExecutorService syncControl;
    private ExecutorService persistWorker;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        loadSettings();
        // Persists per-player YAML off the region threads (they must never do disk
        // I/O). The store coalesces writes onto this single thread.
        persistWorker = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "SparkleMorpherBridge-persist");
            thread.setDaemon(true);
            return thread;
        });
        try {
            modelStore = new ModelStore(getDataFolder().toPath(), getConfig().getInt("max-model-bytes", 67_108_864),
                    getConfig().getInt("upload-timeout-seconds", 300), persistWorker);
        } catch (Exception error) {
            throw new IllegalStateException("Unable to initialize Sparkle model storage", error);
        }
        storageWorker = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "SparkleMorpherBridge-storage");
            thread.setDaemon(true);
            return thread;
        });
        // Downloads stream on their own pool so a big upload compile on the
        // storage worker can never stall a sync (or vice versa).
        int downloadThreads = Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors() / 2));
        downloadWorker = Executors.newFixedThreadPool(downloadThreads, runnable -> {
            Thread thread = new Thread(runnable, "SparkleMorpherBridge-download");
            thread.setDaemon(true);
            return thread;
        });
        // Handshake/request parsing is small and latency sensitive; it must not
        // queue behind parked model-stream threads, so it gets its own pair.
        syncControl = Executors.newFixedThreadPool(2, runnable -> {
            Thread thread = new Thread(runnable, "SparkleMorpherBridge-sync");
            thread.setDaemon(true);
            return thread;
        });
        catalogReady = false;
        legacySync = new LegacySyncService(modelStore, this::queuePayload, this::findPlayer, this::pendingBytes,
                downloadWorker, downloadBacklogBytes, () -> catalogReady);
        modelCompiler = new ModelCompiler(modelStore);
        storageWorker.execute(this::compileStoredModels);
        getServer().getMessenger().registerIncomingPluginChannel(this, SparkleWire.CHANNEL, this);
        getServer().getMessenger().registerOutgoingPluginChannel(this, SparkleWire.CHANNEL);
        getServer().getPluginManager().registerEvents(this, this);
        PluginCommand command = getCommand("sparklemorpher");
        if (command != null) {
            command.setExecutor(this);
        }
        getLogger().info("Sparkle Morpher protocol bridge enabled for channel " + SparkleWire.CHANNEL + ".");
    }

    @Override
    public void onDisable() {
        negotiatedClients.clear();
        outboundPayloads.clear();
        outboundDrains.clear();
        outboundPending.clear();
        trackedBy.clear();
        stateMonitors.values().forEach(ScheduledTask::cancel);
        stateMonitors.clear();
        knownFlying.clear();
        knownFood.clear();
        if (storageWorker != null) storageWorker.shutdownNow();
        if (downloadWorker != null) downloadWorker.shutdownNow();
        if (syncControl != null) syncControl.shutdownNow();
        if (modelStore != null) modelStore.flushPlayerStateSaves();
        if (persistWorker != null) persistWorker.shutdownNow();
        getServer().getMessenger().unregisterIncomingPluginChannel(this);
        getServer().getMessenger().unregisterOutgoingPluginChannel(this);
    }

    @EventHandler
    public void onPlayerRegisterChannel(PlayerRegisterChannelEvent event) {
        if (!SparkleWire.CHANNEL.equals(event.getChannel())) {
            return;
        }
        Player player = event.getPlayer();
        player.getScheduler().run(this, ignored -> sendHandshake(player), () -> { });
    }

    @EventHandler
    public void onPlayerQuit(PlayerQuitEvent event) {
        Player quitter = event.getPlayer();
        resetStateMonitor(quitter);
        UUID playerId = quitter.getUniqueId();
        negotiatedClients.remove(playerId);
        outboundPayloads.remove(playerId);
        outboundDrains.remove(playerId);
        outboundPending.remove(playerId);
        trackedBy.remove(playerId);
        trackedBy.values().forEach(receivers -> receivers.remove(playerId));
        if (legacySync != null) legacySync.remove(quitter);
    }

    @EventHandler
    public void onPlayerTrackEntity(PlayerTrackEntityEvent event) {
        if (!(event.getEntity() instanceof Player tracked)) return;
        trackedBy.computeIfAbsent(tracked.getUniqueId(), ignored -> ConcurrentHashMap.newKeySet()).add(event.getPlayer().getUniqueId());
        if (!isNegotiated(event.getPlayer())) return;
        queueModelState(tracked, event.getPlayer());
    }

    @EventHandler
    public void onPlayerUntrackEntity(PlayerUntrackEntityEvent event) {
        if (event.getEntity() instanceof Player tracked) {
            Set<UUID> receivers = trackedBy.get(tracked.getUniqueId());
            if (receivers != null) receivers.remove(event.getPlayer().getUniqueId());
        }
    }

    @Override
    public void onPluginMessageReceived(String channel, Player player, byte[] body) {
        if (!SparkleWire.CHANNEL.equals(channel)) {
            return;
        }
        try {
            SparkleWire.ClientMessage message = SparkleWire.decodeClientMessage(body);
            if (message instanceof SparkleWire.VersionCheck versionCheck) {
                negotiatedClients.put(player.getUniqueId(), versionCheck.version());
                player.getScheduler().run(this, ignored -> {
                    // A fresh handshake supersedes any stale session; drop leftover
                    // payloads first so old-key chunks can never reach a client that
                    // is re-negotiating keys. If a download is still producing for
                    // this player, leave it untouched.
                    if (!legacySync.isBusy(player)) {
                        clearPayloadQueue(player.getUniqueId());
                        legacySync.start(player);
                    }
                    // Mirror C2SVersionCheckPacket.handle: restart the entity-state
                    // delta monitor and re-push the server-persisted star list so a
                    // reconnect converges on the authoritative set.
                    resetStateMonitor(player);
                    sendPolicy(player);
                    sendStars(player);
                    sendKnownSelections(player);
                    startStateMonitor(player);
                }, () -> { });
            } else if (!isNegotiated(player)) {
                return;
            } else if (message instanceof SparkleWire.LegacySyncPayload payload) {
                syncControl.execute(() -> legacySync.accept(player, payload.data()));
            } else if (message instanceof SparkleWire.UploadStart upload) {
                player.getScheduler().run(this, ignored -> handleUploadStart(player, upload), () -> { });
            } else if (message instanceof SparkleWire.UploadChunk upload) {
                storageWorker.execute(() -> modelStore.append(player.getUniqueId(), upload.uploadId(), upload.offset(), upload.data()));
            } else if (message instanceof SparkleWire.UploadFinish upload) {
                handleUploadFinish(player, upload);
            } else if (message instanceof SparkleWire.ModelSelection selection) {
                player.getScheduler().run(this, ignored -> handleModelSelection(player, selection), () -> { });
            } else if (message instanceof SparkleWire.CompleteFeedback feedback) {
                player.getScheduler().run(this, ignored -> handleCompleteFeedback(player, feedback), () -> { });
            } else if (message instanceof SparkleWire.RequestExecuteMolang request) {
                player.getScheduler().run(this, ignored -> handleExecuteMolang(player, request), () -> { });
            } else if (message instanceof SparkleWire.SetStarModel star) {
                player.getScheduler().run(this, ignored -> handleStarModel(player, star), () -> { });
            } else if (message instanceof SparkleWire.PlayAnimation animation) {
                player.getScheduler().run(this, ignored -> handlePlayAnimation(player, animation), () -> { });
            } else if (message instanceof SparkleWire.AnimationExpression expression) {
                player.getScheduler().run(this, ignored -> broadcastAnimationExpression(player, expression.values()), () -> { });
            } else if (message instanceof SparkleWire.SwingArm swing) {
                player.getScheduler().run(this, ignored -> {
                    if (swing.offHand()) player.swingOffHand(); else player.swingMainHand();
                }, () -> { });
            }
        } catch (IllegalArgumentException error) {
            getLogger().log(Level.FINE, "Rejected malformed Sparkle Morpher payload from " + player.getName(), error);
        }
    }

    public boolean isNegotiated(Player player) {
        return negotiatedClients.containsKey(player.getUniqueId());
    }

    private void sendHandshake(Player player) {
        if (!player.isOnline() || !player.getListeningPluginChannels().contains(SparkleWire.CHANNEL)) {
            return;
        }
        player.sendPluginMessage(this, SparkleWire.CHANNEL, SparkleWire.encodeServerVersionCheck(allowModelUpload));
    }

    private void sendPolicy(Player player) {
        if (!player.isOnline() || !player.getListeningPluginChannels().contains(SparkleWire.CHANNEL)) {
            return;
        }
        player.sendPluginMessage(this, SparkleWire.CHANNEL, SparkleWire.encodeEmptyAuthorizationList());
    }

    private void handleUploadStart(Player player, SparkleWire.UploadStart upload) {
        ModelStore.UploadStart result = modelStore.begin(player.getUniqueId(),
                allowModelUpload && player.hasPermission("sparklemorpher.upload"), upload.modelId(), upload.fileName(),
                upload.totalBytes(), upload.sha256());
        sendPayload(player, SparkleWire.encodeUploadStart(result.id(), result.status(), uploadChunkBytes,
                getConfig().getInt("max-model-bytes", 67_108_864), uploadChunksPerTick, result.message()));
    }

    private void handleUploadFinish(Player player, SparkleWire.UploadFinish upload) {
        storageWorker.execute(() -> {
            ModelStore.UploadResult result = modelStore.finish(player.getUniqueId(), upload.uploadId());
            byte status = result.status();
            String message = result.message();
            long hash1 = 0L;
            long hash2 = 0L;
            if (status == 0) {
                try {
                    ModelStore.CompiledModel compiled = modelCompiler.compile(modelStore.find(result.modelId()));
                    hash1 = compiled.hash1();
                    hash2 = compiled.hash2();
                } catch (Exception error) {
                    getLogger().log(Level.WARNING, "Failed to compile uploaded Sparkle model " + result.modelId(), error);
                    status = 8;
                    message = "Model compilation failed: " + userFacingError(error);
                }
            }
            byte finalStatus = status;
            String finalMessage = message;
            long finalHash1 = hash1;
            long finalHash2 = hash2;
            player.getScheduler().run(this, ignored -> sendPayload(player,
                    SparkleWire.encodeUploadResult(result.id(), finalStatus, result.modelId(), finalHash1, finalHash2, finalMessage)), () -> { });
            if (finalStatus == 0) {
                // A successful upload added a model to the catalog; give every
                // connected client a fresh sync so the new model becomes visible
                // (mirrors the official reloadModelsAfterImport -> sync-all).
                resyncAllPlayers();
            }
        });
    }

    /**
     * Starts a fresh legacy model sync for every negotiated, online, non-busy
     * player so a catalog change (e.g. a new upload) reaches already-synced
     * clients without a reconnect. Runs on each player's own region scheduler.
     */
    private void resyncAllPlayers() {
        for (Player player : getServer().getOnlinePlayers()) {
            if (!isNegotiated(player)) continue;
            player.getScheduler().run(this, ignored -> {
                if (legacySync != null && !legacySync.isBusy(player)) legacySync.start(player);
            }, () -> { });
        }
    }

    /**
     * Re-pushes every visible player's model-state frame to each negotiated client.
     * Used once after the boot catalog becomes ready, because a client that joined
     * during compilation saw an empty model list at handshake and needs the real
     * states now. Scheduled on each recipient's own region scheduler.
     */
    private void pushKnownSelectionsToAll() {
        for (Player player : getServer().getOnlinePlayers()) {
            if (!isNegotiated(player)) continue;
            player.getScheduler().run(this, ignored -> sendKnownSelections(player), () -> { });
        }
    }

    private static String userFacingError(Exception error) {
        String message = error.getMessage();
        if (message == null || message.isBlank()) return error.getClass().getSimpleName();
        return message.length() > 180 ? message.substring(0, 180) : message;
    }

    private void compileStoredModels() {
        try {
            int compiled = 0;
            int unchanged = 0;
            int corrupt = 0;
            int failed = 0;
            for (ModelStore.ModelFile model : modelStore.models()) {
                // Unchanged since last successful compile: restore from catalog, no decode.
                if (modelStore.isUpToDate(model.modelId(), model.sha256())) {
                    unchanged++;
                    continue;
                }
                // Same bytes that already failed once: do not re-flood the log every boot.
                if (modelStore.isKnownBad(model.modelId(), model.sha256())) {
                    corrupt++;
                    getLogger().info("Skipped unchanged corrupt Sparkle model " + model.fileName()
                            + " (re-upload a fixed copy to retry).");
                    continue;
                }
                try {
                    modelCompiler.compile(model);
                    modelStore.clearFailure(model.modelId());
                    compiled++;
                } catch (Exception error) {
                    failed++;
                    modelStore.rememberFailure(model.modelId(), model.sha256());
                    getLogger().log(Level.WARNING, "Skipped Sparkle model " + model.fileName(), error);
                }
            }
            getLogger().info("Sparkle model scan complete: compiled=" + compiled + ", unchanged=" + unchanged
                    + ", corrupt-skipped=" + corrupt + ", failed=" + failed + ".");
        } finally {
            // The catalog is now final for this boot (even if a model failed to
            // parse). Mark it ready and flush any session that was parked
            // mid-handshake so its client leaves "preparing cache" with the
            // complete model list instead of an empty one.
            catalogReady = true;
            if (legacySync != null) legacySync.notifyCatalogReady();
            pushKnownSelectionsToAll();
        }
    }

    private void handleModelSelection(Player player, SparkleWire.ModelSelection selection) {
        if (modelStore.compiled(selection.modelId()) == null) return;
        modelStore.setSelection(player.getUniqueId(), selection.modelId(), selection.textureId());
        sendToTrackingAndSelf(player, SparkleWire.encodeModelState(player.getEntityId(), selection.modelId(),
                selection.textureId(), null, player.isFlying(), player.getFoodLevel()));
    }

    private void handleCompleteFeedback(Player player, SparkleWire.CompleteFeedback feedback) {
        if (feedback.entityId() != player.getEntityId() || feedback.values().isEmpty()) return;
        ModelStore.CompiledModel selected = modelStore.compiled(modelStore.selection(player.getUniqueId()).modelId());
        if (selected == null || selected.modelHashId() != feedback.modelHashId()) return;
        modelStore.applyMolangState(player.getUniqueId(), feedback.modelHashId(), feedback.values());
        sendToTrackingAndSelf(player, SparkleWire.encodeMolangState(player.getEntityId(), feedback.modelHashId(), feedback.values()));
    }

    private void handleExecuteMolang(Player player, SparkleWire.RequestExecuteMolang request) {
        if (request.entityId() != player.getEntityId() || request.expression().isBlank()) return;
        sendToTrackingAndSelf(player, SparkleWire.encodeExecuteMolang(player.getEntityId(), request.expression()));
    }

    private void handleStarModel(Player player, SparkleWire.SetStarModel star) {
        // Mirror the official handler: persist and echo the star regardless of
        // whether the model is currently on the catalog (the client merges its own
        // local stars against this server-authoritative list).
        sendPayload(player, SparkleWire.encodeStarModels(modelStore.updateStar(player.getUniqueId(), star.modelId(), star.add())));
    }

    private void handlePlayAnimation(Player player, SparkleWire.PlayAnimation animation) {
        // Entity-specific requests are for optional third-party maid support.
        // Player actions always carry -1 and can be relayed unchanged.
        if (animation.entityId() != -1) return;
        String animationKey = animation.animationIndex() < 0 ? "" : animation.animationKey();
        if (animationKey.length() > 512) return;
        sendToTrackingAndSelf(player, SparkleWire.encodeAnimationState(player.getEntityId(), animationKey));
    }

    private void broadcastAnimationExpression(Player player, float[] values) {
        sendToTrackingAndSelf(player, SparkleWire.encodeAnimationExpression(player.getEntityId(), values));
    }

    private void sendKnownSelections(Player recipient) {
        for (Player player : getServer().getOnlinePlayers()) {
            queueModelState(player, recipient);
        }
    }

    /**
     * Sends the source's current model-state frame to the recipient. Entity-state
     * fields (flight/hunger) are read on the source's own region scheduler and the
     * payload is then queued onto the recipient's, so no cross-region property reads
     * occur. A no-op when the source has no compiled model.
     */
    private void queueModelState(Player source, Player recipient) {
        if (source == null || recipient == null || !recipient.isOnline() || !isNegotiated(recipient)) return;
        ModelStore.Selection selection = modelStore.selection(source.getUniqueId());
        if (modelStore.compiled(selection.modelId()) == null) return;
        SparkleWire.MolangState molang = toWireState(source.getUniqueId(), selection.modelId());
        if (source.getUniqueId().equals(recipient.getUniqueId())) {
            // Same player: we are already running on the recipient's scheduler.
            sendPayload(recipient, SparkleWire.encodeModelState(source.getEntityId(), selection.modelId(),
                    selection.textureId(), molang, source.isFlying(), source.getFoodLevel()));
            return;
        }
        source.getScheduler().run(this, ignored -> {
            if (!source.isOnline()) return;
            byte[] payload = SparkleWire.encodeModelState(source.getEntityId(), selection.modelId(),
                    selection.textureId(), molang, source.isFlying(), source.getFoodLevel());
            recipient.getScheduler().run(this, ignored2 -> sendPayload(recipient, payload), () -> { });
        }, () -> { });
    }

    private void sendStars(Player player) {
        if (!player.isOnline() || !player.getListeningPluginChannels().contains(SparkleWire.CHANNEL)) return;
        sendPayload(player, SparkleWire.encodeStarModels(modelStore.stars(player.getUniqueId())));
    }

    /**
     * Schedules the per-player fixed-rate sweep that turns flight/hunger changes into
     * S2C-21 deltas. Runs on the player's own region via the entity scheduler; the
     * initial 20-tick delay lets the handshake snapshots reach watchers first so the
     * first sweep only seeds the baseline and never emits a redundant delta.
     */
    private void startStateMonitor(Player player) {
        if (player == null || !player.isOnline()) return;
        UUID playerId = player.getUniqueId();
        if (stateMonitors.containsKey(playerId)) return;
        // Seed the baseline to the values just pushed in the handshake snapshots so
        // the first sweep only compares against the current state (no redundant
        // delta, and a change inside the initial-delay window is still detected).
        knownFlying.putIfAbsent(playerId, player.isFlying());
        knownFood.putIfAbsent(playerId, player.getFoodLevel());
        ScheduledTask task = player.getScheduler().runAtFixedRate(this,
                ignored -> tickPlayerState(player), () -> { }, 20L, 10L);
        if (task != null) stateMonitors.put(playerId, task);
    }

    private void resetStateMonitor(Player player) {
        if (player == null) return;
        UUID playerId = player.getUniqueId();
        ScheduledTask task = stateMonitors.remove(playerId);
        if (task != null) task.cancel();
        knownFlying.remove(playerId);
        knownFood.remove(playerId);
    }

    private void tickPlayerState(Player player) {
        if (player == null || !player.isOnline() || !negotiatedClients.containsKey(player.getUniqueId())) return;
        UUID playerId = player.getUniqueId();
        boolean flying = player.isFlying();
        int food = player.getFoodLevel();
        Boolean lastFlying = knownFlying.putIfAbsent(playerId, flying);
        if (lastFlying != null && lastFlying != flying) {
            knownFlying.put(playerId, flying);
            sendToTrackingAndSelf(player, SparkleWire.encodePlayerStateFlying(player.getEntityId(), flying));
        }
        Integer lastFood = knownFood.putIfAbsent(playerId, food);
        if (lastFood != null && lastFood != food) {
            knownFood.put(playerId, food);
            sendToTrackingAndSelf(player, SparkleWire.encodePlayerStateFood(player.getEntityId(), food));
        }
    }

    private SparkleWire.MolangState toWireState(UUID playerId, String modelId) {
        ModelStore.MolangState state = modelStore.molangState(playerId);
        ModelStore.CompiledModel model = modelStore.compiled(modelId);
        if (state == null || model == null || state.modelHashId() != model.modelHashId()) return null;
        return state == null ? null : new SparkleWire.MolangState(state.modelHashId(), state.values());
    }

    private boolean sendPayload(Player player, byte[] payload) {
        if (payload == null || !player.isOnline() || !player.getListeningPluginChannels().contains(SparkleWire.CHANNEL)) {
            return false;
        }
        if (payload.length > SparkleWire.PLUGIN_MESSAGE_LIMIT) {
            // Bukkit refuses frames over 1 MiB; no delivery path exists for one, so
            // drop it instead of letting the exception escape and wedge the queue.
            // Callers treat the payload as consumed either way (the manifest trims
            // pack icons so this should never fire on the sync path).
            getLogger().warning("Dropped " + payload.length + "-byte frame to " + player.getName()
                    + ": exceeds the " + SparkleWire.PLUGIN_MESSAGE_LIMIT + "-byte plugin-message limit.");
            return false;
        }
        try {
            player.sendPluginMessage(this, SparkleWire.CHANNEL, payload);
            return true;
        } catch (RuntimeException error) {
            // Never let a send failure kill the entity task draining this player's
            // queue; otherwise the drain marker is left set and the queue wedges.
            getLogger().log(Level.WARNING, "Failed to send plugin message to " + player.getName(), error);
            return false;
        }
    }

    private Player findPlayer(UUID playerId) {
        return getServer().getPlayer(playerId);
    }

    private long pendingBytes(UUID playerId) {
        AtomicLong pending = outboundPending.get(playerId);
        return pending == null ? 0L : pending.get();
    }

    /** Equivalent to NetworkHandler.sendToTrackingEntityAndSelf for player state. */
    private void sendToTrackingAndSelf(Player source, byte[] payload) {
        Set<UUID> receivers = new HashSet<>();
        receivers.add(source.getUniqueId());
        receivers.addAll(trackedBy.getOrDefault(source.getUniqueId(), Set.of()));
        for (UUID receiverId : receivers) {
            Player receiver = getServer().getPlayer(receiverId);
            if (receiver == null || !isNegotiated(receiver)) continue;
            receiver.getScheduler().run(this, ignored -> sendPayload(receiver, payload), () -> { });
        }
    }

    private void queuePayload(Player player, byte[] payload) {
        if (player == null || payload == null) return;
        UUID playerId = player.getUniqueId();
        outboundPayloads.computeIfAbsent(playerId, ignored -> new ConcurrentLinkedQueue<>()).add(payload);
        outboundPending.computeIfAbsent(playerId, ignored -> new AtomicLong()).addAndGet(payload.length);
        if (outboundDrains.putIfAbsent(playerId, Boolean.TRUE) == null) {
            player.getScheduler().run(this, ignored -> drainPayloads(player), () -> clearPayloadQueue(playerId));
        }
    }

    private void drainPayloads(Player player) {
        UUID playerId = player.getUniqueId();
        if (!player.isOnline()) {
            clearPayloadQueue(playerId);
            return;
        }
        ConcurrentLinkedQueue<byte[]> queue = outboundPayloads.get(playerId);
        if (queue == null || queue.isEmpty()) {
            outboundDrains.remove(playerId);
            // A worker may have queued a new packet between poll() and the
            // empty check. Re-arm the one-at-a-time drain without dropping it.
            if (queue != null && !queue.isEmpty() && outboundDrains.putIfAbsent(playerId, Boolean.TRUE) == null) {
                player.getScheduler().run(this, ignored -> drainPayloads(player), () -> clearPayloadQueue(playerId));
            }
            return;
        }
        // Reproduce the official server's flow control: stop writing while the
        // client's transport is full. When the stock client's sync worker falls
        // behind, TCP backpressure unwrites our netty outbound past its high
        // watermark and isWritable() flips false, so we pause instead of flooding
        // the client's bounded packet queue (which would stall its main thread
        // past the 30 s keepalive window and get it kicked).
        if (!isChannelWritable(player)) {
            player.getScheduler().run(this, ignored -> drainPayloads(player), () -> clearPayloadQueue(playerId));
            return;
        }
        int budget = Math.max(1, downloadChunksPerTick);
        for (int sent = 0; sent < budget; sent++) {
            byte[] payload = queue.poll();
            if (payload == null) break;
            // The payload has left the queue either way; keep the pending-byte
            // counter in sync with the queue so the streaming producer's
            // backpressure can never deadlock on undeliverable data.
            AtomicLong pending = outboundPending.get(playerId);
            if (pending != null) pending.addAndGet(-payload.length);
            if (!sendPayload(player, payload)) break;
        }
        if (queue.isEmpty()) {
            outboundDrains.remove(playerId);
            // A worker may have queued a new packet between poll() and the
            // empty check. Re-arm the one-at-a-time drain without dropping it.
            if (!queue.isEmpty() && outboundDrains.putIfAbsent(playerId, Boolean.TRUE) == null) {
                player.getScheduler().run(this, ignored -> drainPayloads(player), () -> clearPayloadQueue(playerId));
            }
            return;
        }
        player.getScheduler().run(this, ignored -> drainPayloads(player), () -> clearPayloadQueue(playerId));
    }

    /**
     * Whether the player's netty channel can accept more outbound bytes. Resolved
     * reflectively from the CraftBukkit/NMS connection because the plugin only
     * compiles against the paper-api. If the internals can't be reached (renamed
     * fields, non-CraftBukkit implementation) it degrades to true and the plugin
     * relies on the per-tick budget alone.
     */
    private boolean isChannelWritable(Player player) {
        try {
            Object handle = invokeMethod(player, "getHandle");
            if (handle == null) return true;
            Object packetListener = readField(handle, "connection");
            if (packetListener == null) return true;
            Object connection = readField(packetListener, "connection");
            if (connection == null) return true;
            Object channel = readField(connection, "channel");
            if (channel == null) return true;
            return (boolean) invokeMethod(channel, "isWritable");
        } catch (Throwable ignored) {
            return true;
        }
    }

    private static Object invokeMethod(Object target, String name) throws ReflectiveOperationException {
        try {
            java.lang.reflect.Method method = target.getClass().getMethod(name);
            method.setAccessible(true);
            return method.invoke(target);
        } catch (NoSuchMethodException ignored) {
            // fall through to the declared-method walk for non-public methods
        }
        for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
            try {
                java.lang.reflect.Method method = type.getDeclaredMethod(name);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (NoSuchMethodException ignored) {
                // continue up the hierarchy
            }
        }
        return null;
    }

    private static Object readField(Object target, String name) throws ReflectiveOperationException {
        for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
            try {
                java.lang.reflect.Field field = type.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException ignored) {
                // continue up the hierarchy
            }
        }
        return null;
    }

    private void clearPayloadQueue(UUID playerId) {
        outboundPayloads.remove(playerId);
        outboundDrains.remove(playerId);
        outboundPending.remove(playerId);
    }

    private void loadSettings() {
        reloadConfig();
        allowModelUpload = getConfig().getBoolean("allow-model-upload", false);
        uploadChunkBytes = Math.max(1024, Math.min(32_000, getConfig().getInt("upload-chunk-bytes", 30_720)));
        uploadChunksPerTick = Math.max(1, Math.min(32, getConfig().getInt("upload-chunks-per-tick", 4)));
        downloadChunksPerTick = Math.max(1, Math.min(64, getConfig().getInt("download-chunks-per-tick", 16)));
        downloadBacklogBytes = Math.max(64 * 1024L,
                Math.min(8 * 1024 * 1024L, getConfig().getLong("download-backlog-bytes", 786_432L)));
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (args.length == 0 || args[0].equalsIgnoreCase("status")) {
            sender.sendMessage("[SparkleMorpherBridge] Negotiated clients: " + negotiatedClients.size()
                + ", compiled models: " + modelStore.compiledModels().size() + ", model upload: " + allowModelUpload);
            return true;
        }
        if (args[0].equalsIgnoreCase("reload")) {
            if (!sender.hasPermission("sparklemorpher.admin")) {
                sender.sendMessage("You do not have permission.");
                return true;
            }
            loadSettings();
            sender.sendMessage("[SparkleMorpherBridge] Configuration reloaded.");
            return true;
        }
        sender.sendMessage("Usage: /sparklemorpher [status|reload]");
        return true;
    }
}
