package local.nanoda.sparklemorpher;

import local.nanoda.sparklemorpher.protocol.SparkleWire;
import local.nanoda.sparklemorpher.protocol.LegacySyncService;
import local.nanoda.sparklemorpher.storage.ModelStore;
import local.nanoda.sparklemorpher.storage.ModelCompiler;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.PluginCommand;
import org.bukkit.command.TabCompleter;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
public final class SparkleMorpherBridgePlugin extends JavaPlugin implements Listener, PluginMessageListener, CommandExecutor, TabCompleter {
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
    /** Serializes admin catalog jobs (rescan/rebuild/prune/delete) so they cannot interleave. */
    private final java.util.concurrent.atomic.AtomicBoolean catalogMaintenanceRunning = new java.util.concurrent.atomic.AtomicBoolean();
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
            command.setTabCompleter(this);
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

    // ---- permission nodes ----
    private static final String PERM_COMMAND = "sparklemorpher.command";
    private static final String PERM_READ = "sparklemorpher.read";
    private static final String PERM_MODIFY = "sparklemorpher.modify";
    private static final String PERM_DESTRUCTIVE = "sparklemorpher.destructive";
    private static final String PREFIX = "[SM] ";

    /** Sends command feedback, hopping to the sender's region when the sender is a player. */
    private void msg(CommandSender sender, String text) {
        if (sender instanceof Player player) {
            player.getScheduler().run(this, ignored -> player.sendMessage(text), () -> { });
        } else {
            sender.sendMessage(text);
        }
    }

    private boolean requirePermission(CommandSender sender, String node) {
        if (sender.hasPermission(node) || sender.hasPermission("sparklemorpher.admin")) return true;
        sender.sendMessage(PREFIX + "你没有权限执行该操作(" + node + ")。");
        return false;
    }

    private static boolean hasFlag(String[] args, String flag) {
        for (int i = 1; i < args.length; i++) if (args[i].equalsIgnoreCase(flag)) return true;
        return false;
    }

    /** Deterministic short confirmation token so a copy-pasted confirmation cannot hit the wrong subject. */
    private static String confirmToken(String action, String subject) {
        try {
            byte[] d = java.security.MessageDigest.getInstance("SHA-256")
                    .digest((action + ":" + subject).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(6);
            for (int i = 0; i < 3; i++) sb.append(String.format("%02x", d[i]));
            return sb.toString();
        } catch (Exception e) {
            return "confirm";
        }
    }

    /** True when the caller already supplied the expected token; otherwise prints how to proceed. */
    private boolean requireConfirmation(CommandSender sender, String[] args, String token, String description) {
        if (hasFlag(args, token)) return true;
        sender.sendMessage(PREFIX + "该操作不可逆:" + description);
        sender.sendMessage(PREFIX + "确认请在命令末尾附加令牌: " + token);
        return false;
    }

    /** Runs a catalog job on the storage worker, one at a time. Job bodies may do blocking I/O. */
    private void runCatalogJob(CommandSender sender, String name, Runnable job) {
        if (!catalogMaintenanceRunning.compareAndSet(false, true)) {
            sender.sendMessage(PREFIX + "已有目录任务正在执行,请稍后重试。");
            return;
        }
        sender.sendMessage(PREFIX + "开始执行:" + name + "(后台进行,完成后提示)");
        storageWorker.execute(() -> {
            try {
                job.run();
                msg(sender, PREFIX + "已完成:" + name);
            } catch (Throwable error) {
                getLogger().log(Level.WARNING, "Catalog job failed: " + name, error);
                msg(sender, PREFIX + "任务失败:" + name + " -> " + error);
            } finally {
                catalogMaintenanceRunning.set(false);
            }
        });
    }

    /** Re-syncs every connected client so catalog changes (new/deleted models) become visible. */
    private void notifyCatalogChanged() {
        resyncAllPlayers();
        pushKnownSelectionsToAll();
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (args.length == 0 || args[0].equalsIgnoreCase("status")) {
            if (!requirePermission(sender, PERM_READ)) return true;
            statusCommand(sender);
            return true;
        }
        String sub = args[0].toLowerCase(Locale.ROOT);
        switch (sub) {
            case "reload" -> { if (requirePermission(sender, PERM_MODIFY)) { loadSettings(); sender.sendMessage(PREFIX + "配置已重新加载。"); } }
            case "list", "models" -> { if (requirePermission(sender, PERM_READ)) listModels(sender, args); }
            case "packs" -> { if (requirePermission(sender, PERM_READ)) listPacks(sender); }
            case "model" -> { if (requirePermission(sender, PERM_READ)) modelInfo(sender, args); }
            case "get" -> { if (requirePermission(sender, PERM_READ)) getState(sender, args); }
            case "uploads" -> { if (requirePermission(sender, PERM_READ)) uploadsCommand(sender); }
            case "cache" -> { if (requirePermission(sender, PERM_READ)) cacheCommand(sender); }
            case "stats" -> { if (requirePermission(sender, PERM_READ)) statsCommand(sender); }
            case "session" -> { if (requirePermission(sender, PERM_READ)) sessionCommand(sender, args); }
            case "stars" -> { if (requirePermission(sender, PERM_READ)) starsCommand(sender, args); }
            case "verify" -> { if (requirePermission(sender, PERM_MODIFY)) verifyCommand(sender, args); }
            case "set" -> { if (requirePermission(sender, PERM_MODIFY)) setModel(sender, args); }
            case "setall" -> { if (requirePermission(sender, PERM_MODIFY)) setAllCommand(sender, args); }
            case "clear" -> { if (requirePermission(sender, PERM_MODIFY)) clearModel(sender, args); }
            case "star" -> { if (requirePermission(sender, PERM_MODIFY)) starCommand(sender, args); }
            case "rescan" -> { if (requirePermission(sender, PERM_MODIFY)) rescanCommand(sender); }
            case "recompile" -> { if (requirePermission(sender, PERM_MODIFY)) recompileCommand(sender, args); }
            case "resync" -> { if (requirePermission(sender, PERM_MODIFY)) resyncCommand(sender, args); }
            case "packicon" -> { if (requirePermission(sender, PERM_READ)) packIconCommand(sender, args); }
            case "delete" -> { if (requirePermission(sender, PERM_DESTRUCTIVE)) deleteCommand(sender, args); }
            case "prune" -> { if (requirePermission(sender, PERM_DESTRUCTIVE)) pruneCommand(sender, args); }
            case "rebuild" -> { if (requirePermission(sender, PERM_DESTRUCTIVE)) rebuildCommand(sender, args); }
            case "reset" -> { if (requirePermission(sender, PERM_DESTRUCTIVE)) resetCommand(sender, args); }
            default -> sender.sendMessage(PREFIX + "用法: /sparklemorpher <status|list|packs|model|get|uploads|cache|stats|session|stars|set|setall|clear|star|rescan|recompile|resync|packicon|verify|reload|delete|prune|rebuild|reset>");
        }
        return true;
    }

    // ---- diagnostics ----

    private void statusCommand(CommandSender sender) {
        ModelStore.CacheStats cache = modelStore.cacheStats();
        sender.sendMessage(PREFIX + "已协商客户端=" + negotiatedClients.size()
                + " 已编译模型=" + modelStore.compiledModels().size()
                + " 源模型=" + modelStore.models().size()
                + " 分类=" + modelStore.packs().size());
        sender.sendMessage(PREFIX + "上传开关=" + allowModelUpload
                + " 目录就绪=" + catalogReady
                + " 缓存文件=" + cache.files() + "(" + (cache.totalBytes() / 1024 / 1024) + " MiB)"
                + " 孤儿=" + cache.orphans()
                + " 缺缓存条目=" + cache.missingForCatalog());
    }

    private void listModels(CommandSender sender, String[] args) {
        List<ModelStore.CompiledModel> models = modelStore.compiledModels();
        String query = null;
        int page = 1;
        for (int i = 1; i < args.length; i++) {
            try { page = Math.max(1, Integer.parseInt(args[i])); }
            catch (NumberFormatException e) { query = args[i].toLowerCase(Locale.ROOT); }
        }
        if (query != null) {
            final String q = query;
            models = models.stream().filter(m -> m.modelId().toLowerCase(Locale.ROOT).contains(q)).toList();
        }
        int perPage = 20;
        int pages = Math.max(1, (models.size() + perPage - 1) / perPage);
        page = Math.min(page, pages);
        sender.sendMessage(PREFIX + "模型共 " + models.size() + " 个(第 " + page + "/" + pages + " 页"
                + (query == null ? "" : ",关键字=" + query) + ")");
        int start = (page - 1) * perPage;
        for (int i = start; i < Math.min(models.size(), start + perPage); i++) {
            ModelStore.CompiledModel m = models.get(i);
            boolean hasCache = java.nio.file.Files.isRegularFile(m.cacheFile());
            sender.sendMessage("  " + m.modelId() + (m.auth() ? " [auth]" : "") + (hasCache ? "" : " [缺缓存]"));
        }
    }

    private void listPacks(CommandSender sender) {
        List<ModelStore.ModelPack> packs = modelStore.packs();
        int budget = localIconBudget();
        sender.sendMessage(PREFIX + "分类共 " + packs.size() + " 个(封面帧预算 " + budget + "B)");
        for (ModelStore.ModelPack pack : packs) {
            int icon = pack.iconData() == null ? 0 : pack.iconData().length;
            String fit = icon == 0 ? "无封面" : (icon <= budget ? "封面OK" : "封面过大");
            sender.sendMessage("  " + pack.folderPath() + "  封面=" + icon + "B " + fit
                    + "  名称=" + (pack.name() == null ? "-" : pack.name()));
        }
    }

    private int localIconBudget() {
        return legacySync != null ? LegacySyncService.iconFrameBudget() : SparkleWire.PLUGIN_MESSAGE_LIMIT - 2048;
    }

    private void modelInfo(CommandSender sender, String[] args) {
        if (args.length < 2) { sender.sendMessage(PREFIX + "用法: /sparklemorpher model <模型id>"); return; }
        String id = args[1];
        ModelStore.CompiledModel m = modelStore.compiled(id);
        if (m == null) { sender.sendMessage(PREFIX + "未找到已编译模型: " + id); return; }
        boolean exists = java.nio.file.Files.isRegularFile(m.cacheFile());
        long size = 0L;
        try { if (exists) size = java.nio.file.Files.size(m.cacheFile()); } catch (Exception ignored) { }
        ModelStore.ModelFile source = modelStore.find(m.modelId());
        sender.sendMessage(PREFIX + "模型 " + m.modelId());
        sender.sendMessage("  hash=" + m.modelHash() + "  auth=" + m.auth());
        sender.sendMessage("  hash1=" + String.format("%016x", m.hash1()) + " hash2=" + String.format("%016x", m.hash2()));
        sender.sendMessage("  缓存=" + m.cacheFile().getFileName() + " 存在=" + exists + " 大小=" + size + "B");
        sender.sendMessage("  upToDate=" + (source != null && modelStore.isUpToDate(m.modelId(), source.sha256()))
                + " knownBad=" + (source != null && modelStore.isKnownBad(m.modelId(), source.sha256()))
                + " sourceSha=" + m.sourceSha256());
        sender.sendMessage("  源文件=" + (source == null ? "-" : source.fileName()));
    }

    private void getState(CommandSender sender, String[] args) {
        if (args.length < 2) { sender.sendMessage(PREFIX + "用法: /sparklemorpher get <玩家>"); return; }
        Player target = findOnline(args[1]);
        if (target == null) { sender.sendMessage(PREFIX + "玩家不在线: " + args[1]); return; }
        ModelStore.Selection selection = modelStore.selection(target.getUniqueId());
        ModelStore.MolangState state = modelStore.molangState(target.getUniqueId());
        LegacySyncService.SessionInfo session = legacySync == null
                ? new LegacySyncService.SessionInfo(false, 0, false, false, 0L, 0, false, 0)
                : legacySync.describe(target);
        sender.sendMessage(PREFIX + target.getName()
                + " 模型=" + selection.modelId()
                + " 贴图=" + (selection.textureId().isEmpty() ? "-" : selection.textureId())
                + " 已协商=" + isNegotiated(target)
                + " 星标=" + modelStore.stars(target.getUniqueId()).size()
                + " molang变量=" + (state == null ? 0 : state.values().size()));
        sender.sendMessage("  同步: step=" + session.step() + " busy=" + session.busy()
                + " 清单已发=" + session.manifestSent() + " 待发字节=" + pendingBytes(target.getUniqueId()));
    }

    private void uploadsCommand(CommandSender sender) {
        List<ModelStore.UploadInfo> uploads = modelStore.activeUploads();
        sender.sendMessage(PREFIX + "在途上传 " + uploads.size() + " 个");
        for (ModelStore.UploadInfo u : uploads) {
            sender.sendMessage("  #" + Long.toHexString(u.id()) + " " + u.modelId()
                    + " " + u.receivedBytes() + "/" + u.totalBytes() + "B 空闲" + u.idleMillis() + "ms"
                    + (u.expired() ? " [已超时]" : ""));
        }
    }

    private void cacheCommand(CommandSender sender) {
        ModelStore.CacheStats c = modelStore.cacheStats();
        sender.sendMessage(PREFIX + "缓存: 文件=" + c.files() + " 字节=" + c.totalBytes()
                + " 孤儿文件=" + c.orphans() + "(" + c.orphanBytes() + "B)"
                + " catalog缺缓存=" + c.missingForCatalog());
    }

    private void statsCommand(CommandSender sender) {
        ModelStore.StorageStats s = modelStore.storageStats();
        sender.sendMessage(PREFIX + "存储统计: 源文件=" + s.sourceFiles() + "(" + s.sourceBytes() + "B)"
                + " 已编译=" + s.compiledModels() + " 缓存=" + s.cacheBytes() + "B"
                + " 分类=" + s.packs());
        sender.sendMessage("  玩家状态: selection=" + s.selections() + " 星标集=" + s.starSets()
                + " molang快照=" + s.molangStates());
    }

    private void sessionCommand(CommandSender sender, String[] args) {
        if (args.length < 2) { sender.sendMessage(PREFIX + "用法: /sparklemorpher session <玩家>"); return; }
        Player target = findOnline(args[1]);
        if (target == null) { sender.sendMessage(PREFIX + "玩家不在线: " + args[1]); return; }
        LegacySyncService.SessionInfo s = legacySync.describe(target);
        if (!s.present()) { sender.sendMessage(PREFIX + target.getName() + " 当前没有同步会话。"); return; }
        sender.sendMessage(PREFIX + target.getName() + " 会话: step=" + s.step()
                + " busy=" + s.busy() + " 清单已发=" + s.manifestSent()
                + " 排队hash=" + s.queuedHashes() + " 待处理批次=" + s.pendingBatches());
        sender.sendMessage("  封面: 待补发=" + s.packIcons() + " 已补发=" + s.packIconsResent()
                + " 待发字节=" + pendingBytes(target.getUniqueId()));
    }

    private void starsCommand(CommandSender sender, String[] args) {
        if (args.length < 2) { sender.sendMessage(PREFIX + "用法: /sparklemorpher stars <玩家>"); return; }
        Player target = findOnline(args[1]);
        if (target == null) { sender.sendMessage(PREFIX + "玩家不在线: " + args[1]); return; }
        List<String> ids = new ArrayList<>(modelStore.stars(target.getUniqueId()));
        ids.sort(String::compareTo);
        sender.sendMessage(PREFIX + target.getName() + " 星标 " + ids.size() + " 个:");
        for (String id : ids) {
            sender.sendMessage("  " + id + (modelStore.compiled(id) == null ? " [不在目录]" : ""));
        }
    }

    // ---- model management ----

    /** Region-scoped: must run inside {@code target.getScheduler().run(...)}. */
    private void applySelection(Player target, String modelId, String textureId) {
        modelStore.setSelection(target.getUniqueId(), modelId, textureId);
        knownFlying.put(target.getUniqueId(), target.isFlying());
        knownFood.put(target.getUniqueId(), target.getFoodLevel());
        queueModelState(target, target);
        sendToTrackingAndSelf(target, SparkleWire.encodeModelState(target.getEntityId(), modelId, textureId,
                toWireState(target.getUniqueId(), modelId), target.isFlying(), target.getFoodLevel()));
    }

    /** Region-scoped: must run inside {@code target.getScheduler().run(...)}. */
    private void applyClear(Player target) {
        applySelection(target, "default", "");
    }

    private void setModel(CommandSender sender, String[] args) {
        if (args.length < 3) { sender.sendMessage(PREFIX + "用法: /sparklemorpher set <玩家> <模型id> [贴图id]"); return; }
        Player target = findOnline(args[1]);
        if (target == null) { sender.sendMessage(PREFIX + "玩家不在线: " + args[1]); return; }
        String modelId = args[2];
        if (modelStore.compiled(modelId) == null) { sender.sendMessage(PREFIX + "未知模型(服务端未编译): " + modelId); return; }
        String textureId = args.length >= 4 ? args[3] : "";
        target.getScheduler().run(this, ignored -> applySelection(target, modelId, textureId), () -> { });
        sender.sendMessage(PREFIX + "已将 " + target.getName() + " 设置为 " + modelId
                + (textureId.isEmpty() ? "" : "(" + textureId + ")"));
    }

    private void setAllCommand(CommandSender sender, String[] args) {
        if (args.length < 2) { sender.sendMessage(PREFIX + "用法: /sparklemorpher setall <模型id> [贴图id]"); return; }
        String modelId = args[1];
        if (modelStore.compiled(modelId) == null) { sender.sendMessage(PREFIX + "未知模型(服务端未编译): " + modelId); return; }
        String textureId = args.length >= 3 ? args[2] : "";
        int count = 0;
        for (Player target : getServer().getOnlinePlayers()) {
            target.getScheduler().run(this, ignored -> applySelection(target, modelId, textureId), () -> { });
            count++;
        }
        sender.sendMessage(PREFIX + "已对 " + count + " 名在线玩家设置为 " + modelId);
    }

    private void clearModel(CommandSender sender, String[] args) {
        if (args.length < 2) { sender.sendMessage(PREFIX + "用法: /sparklemorpher clear <玩家>"); return; }
        Player target = findOnline(args[1]);
        if (target == null) { sender.sendMessage(PREFIX + "玩家不在线: " + args[1]); return; }
        target.getScheduler().run(this, ignored -> applyClear(target), () -> { });
        sender.sendMessage(PREFIX + "已将 " + target.getName() + " 重置为默认。");
    }

    private void starCommand(CommandSender sender, String[] args) {
        if (args.length < 4) { sender.sendMessage(PREFIX + "用法: /sparklemorpher star <玩家> <add|remove> <模型id>"); return; }
        Player target = findOnline(args[1]);
        if (target == null) { sender.sendMessage(PREFIX + "玩家不在线: " + args[1]); return; }
        boolean add;
        if (args[2].equalsIgnoreCase("add")) add = true;
        else if (args[2].equalsIgnoreCase("remove")) add = false;
        else { sender.sendMessage(PREFIX + "第二个参数必须是 add 或 remove。"); return; }
        String modelId = args[3];
        Set<String> stars = modelStore.updateStar(target.getUniqueId(), modelId, add);
        target.getScheduler().run(this, ignored -> sendPayload(target, SparkleWire.encodeStarModels(stars)), () -> { });
        sender.sendMessage(PREFIX + (add ? "已为 " : "已取消 ") + target.getName() + " 的星标 " + modelId
                + "(共 " + stars.size() + ")");
    }

    private void rescanCommand(CommandSender sender) {
        if (!catalogReady) { sender.sendMessage(PREFIX + "启动扫描仍在进行,请稍后重试。"); return; }
        runCatalogJob(sender, "增量重扫", () -> {
            compileStoredModels();
            notifyCatalogChanged();
        });
    }

    private void recompileCommand(CommandSender sender, String[] args) {
        if (args.length < 2) { sender.sendMessage(PREFIX + "用法: /sparklemorpher recompile <模型id>"); return; }
        String modelId = args[1];
        runCatalogJob(sender, "重新编译 " + modelId, () -> {
            try {
                ModelStore.ModelFile source = modelStore.find(modelId);
                if (source == null) { msg(sender, PREFIX + "未找到源模型: " + modelId); return; }
                modelCompiler.compile(source);
                msg(sender, PREFIX + "已重新编译: " + modelId);
            } catch (Exception e) {
                msg(sender, PREFIX + "重新编译失败: " + e);
            }
        });
    }

    // ---- destructive ----

    private void verifyCommand(CommandSender sender, String[] args) {
        String target = args.length >= 2 ? args[1] : null;
        if (target != null && target.equalsIgnoreCase("all")) {
            if (!requireConfirmation(sender, args, confirmToken("verify", "all"), "校验全部缓存(读遍所有缓存文件)")) return;
            runCatalogJob(sender, "校验全部缓存", () -> reportVerification(sender, modelStore.verifyCache(null)));
            return;
        }
        runCatalogJob(sender, "校验缓存 " + (target == null ? "全部" : target),
                () -> reportVerification(sender, modelStore.verifyCache(target)));
    }

    private void reportVerification(CommandSender sender, List<ModelStore.CacheVerification> results) {
        int ok = 0;
        List<String> bad = new ArrayList<>();
        for (ModelStore.CacheVerification v : results) {
            if (v.status() == 0) ok++; else bad.add(v.modelId() + " [" + statusText(v.status()) + ":" + v.detail() + "]");
        }
        msg(sender, PREFIX + "校验完成: 通过=" + ok + " 异常=" + bad.size());
        for (String b : bad) msg(sender, "  " + b);
    }

    private static String statusText(byte status) {
        return switch (status) {
            case 0 -> "OK"; case 1 -> "签名不符"; case 2 -> "读取失败"; case 3 -> "文件缺失"; case 4 -> "解密失败"; default -> "未知";
        };
    }

    private void deleteCommand(CommandSender sender, String[] args) {
        if (args.length < 2) { sender.sendMessage(PREFIX + "用法: /sparklemorpher delete <模型id> [--source] [令牌]"); return; }
        String modelId = args[1];
        boolean deleteSource = hasFlag(args, "--source");
        String action = deleteSource ? "delete-source" : "delete";
        if (!requireConfirmation(sender, args, confirmToken(action, modelId),
                "删除模型 " + modelId + (deleteSource ? "(含源文件)" : "(仅派生数据:缓存+catalog行)"))) return;
        runCatalogJob(sender, "删除 " + modelId, () -> {
            try {
                int affected = modelStore.resetSelectionsFor(modelId);
                ModelStore.DeleteResult r = modelStore.deleteModel(modelId, deleteSource);
                msg(sender, PREFIX + "删除结果: catalog=" + r.catalogRowRemoved()
                        + " 缓存=" + r.cacheDeleted() + " 源文件=" + r.sourceDeleted()
                        + " 影响选择=" + Math.max(affected, r.selectionsReset()));
                notifyCatalogChanged();
            } catch (Exception e) {
                msg(sender, PREFIX + "删除失败: " + e);
            }
        });
    }

    private void pruneCommand(CommandSender sender, String[] args) {
        if (!requireConfirmation(sender, args, confirmToken("prune", "cache"), "清理 server-cache 孤儿文件")) return;
        runCatalogJob(sender, "清理孤儿缓存", () -> {
            try {
                ModelStore.PruneResult r = modelStore.pruneCache();
                msg(sender, PREFIX + "清理完成: 删除孤儿=" + r.orphansDeleted()
                        + " 回收=" + r.bytesReclaimed() + "B 丢弃catalog行=" + r.catalogRowsDropped());
            } catch (Exception e) {
                msg(sender, PREFIX + "清理失败: " + e);
            }
        });
    }

    private void rebuildCommand(CommandSender sender, String[] args) {
        if (args.length < 2) { sender.sendMessage(PREFIX + "用法: /sparklemorpher rebuild <模型id|all> [令牌]"); return; }
        String target = args[1];
        if (target.equalsIgnoreCase("all")) {
            if (!requireConfirmation(sender, args, confirmToken("rebuild", "all"),
                    "重建全部缓存(先删除所有 server-cache 再全量重编译,期间请勿禁用插件)")) return;
            runCatalogJob(sender, "全量重建", () -> {
                try {
                    modelStore.resetDerivedCatalog();
                    compileStoredModels();
                    notifyCatalogChanged();
                } catch (Exception e) {
                    msg(sender, PREFIX + "全量重建失败: " + e);
                }
            });
            return;
        }
        if (!requireConfirmation(sender, args, confirmToken("rebuild", target), "重建模型 " + target + " 的缓存")) return;
        runCatalogJob(sender, "重建 " + target, () -> {
            try {
                modelStore.invalidateCompiled(target);
                ModelStore.ModelFile source = modelStore.find(target);
                if (source == null) { msg(sender, PREFIX + "未找到源模型: " + target); return; }
                modelCompiler.compile(source);
                msg(sender, PREFIX + "已重建: " + target);
                notifyCatalogChanged();
            } catch (Exception e) {
                msg(sender, PREFIX + "重建失败: " + e);
            }
        });
    }

    private void resetCommand(CommandSender sender, String[] args) {
        if (args.length < 2) { sender.sendMessage(PREFIX + "用法: /sparklemorpher reset <玩家> [令牌]"); return; }
        Player target = findOnline(args[1]);
        if (target == null) { sender.sendMessage(PREFIX + "玩家不在线: " + args[1]); return; }
        if (!requireConfirmation(sender, args, confirmToken("reset", target.getName()),
                "清空 " + target.getName() + " 的模型选择/星标/molang")) return;
        runCatalogJob(sender, "重置玩家状态 " + target.getName(), () -> {
            String old = modelStore.clearPlayerState(target.getUniqueId());
            msg(sender, PREFIX + "已重置 " + target.getName() + "(原模型=" + old + ")");
        });
        target.getScheduler().run(this, ignored -> applyClear(target), () -> { });
    }

    // ---- packs / sync ----

    private void packIconCommand(CommandSender sender, String[] args) {
        if (args.length < 2) { sender.sendMessage(PREFIX + "用法: /sparklemorpher packicon <分类folder> [reload]"); return; }
        String folder = args[1];
        int budget = localIconBudget();
        ModelStore.ModelPack pack = modelStore.packs().stream()
                .filter(p -> p.folderPath().equalsIgnoreCase(folder) || p.folderPath().equalsIgnoreCase(folder + "/"))
                .findFirst().orElse(null);
        if (pack == null) { sender.sendMessage(PREFIX + "未找到分类: " + folder); return; }
        int icon = pack.iconData() == null ? 0 : pack.iconData().length;
        sender.sendMessage(PREFIX + "分类 " + pack.folderPath() + " 封面=" + icon + "B"
                + " 尺寸=" + pack.iconWidth() + "x" + pack.iconHeight()
                + " 名称=" + (pack.name() == null ? "-" : pack.name())
                + " " + (icon == 0 ? "(无封面)" : (icon <= budget ? "(可下发)" : "(超出帧预算,客户端收不到)")));
        if (!hasFlag(args, "reload")) return;
        if (!requirePermission(sender, PERM_MODIFY)) return;
        if (icon == 0 || icon > budget) { sender.sendMessage(PREFIX + "该封面无法下发,已跳过补发。"); return; }
        int sent = 0;
        for (Player other : getServer().getOnlinePlayers()) {
            if (!isNegotiated(other)) continue;
            other.getScheduler().run(this, ignored ->
                    sendPayload(other, SparkleWire.encodePackIcon(pack.folderPath(), pack.iconData())), () -> { });
            sent++;
        }
        sender.sendMessage(PREFIX + "已向 " + sent + " 名已协商客户端补发该封面。");
    }

    private void resyncCommand(CommandSender sender, String[] args) {
        if (args.length >= 2) {
            Player target = findOnline(args[1]);
            if (target == null) { sender.sendMessage(PREFIX + "玩家不在线: " + args[1]); return; }
            target.getScheduler().run(this, ignored -> {
                if (legacySync != null && !legacySync.isBusy(target)) legacySync.start(target);
            }, () -> { });
            sender.sendMessage(PREFIX + "正在重新同步 " + target.getName() + "。");
            return;
        }
        resyncAllPlayers();
        sender.sendMessage(PREFIX + "正在重新同步所有在线客户端。");
    }

    private Player findOnline(String name) {
        Player player = getServer().getPlayerExact(name);
        return player != null && player.isOnline() ? player : null;
    }

    @Override
    public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
        List<String> out = new ArrayList<>();
        if (args.length == 1) {
            if (sender.hasPermission(PERM_READ) || sender.hasPermission("sparklemorpher.admin")) {
                out.add("status"); out.add("list"); out.add("models"); out.add("packs"); out.add("model");
                out.add("get"); out.add("uploads"); out.add("cache"); out.add("stats"); out.add("session");
                out.add("stars"); out.add("packicon");
            }
            if (sender.hasPermission(PERM_MODIFY) || sender.hasPermission("sparklemorpher.admin")) {
                out.add("set"); out.add("setall"); out.add("clear"); out.add("star");
                out.add("rescan"); out.add("recompile"); out.add("resync"); out.add("verify"); out.add("reload");
            }
            if (sender.hasPermission(PERM_DESTRUCTIVE) || sender.hasPermission("sparklemorpher.admin")) {
                out.add("delete"); out.add("prune"); out.add("rebuild"); out.add("reset");
            }
        } else {
            String sub = args[0].toLowerCase(Locale.ROOT);
            if (args.length == 2) {
                switch (sub) {
                    case "get", "set", "clear", "reset", "session", "stars", "star", "resync" -> {
                        for (Player p : getServer().getOnlinePlayers()) out.add(p.getName());
                    }
                    case "model", "recompile", "delete", "setall" -> addModelIds(out, args[1]);
                    case "rebuild", "verify" -> { out.add("all"); addModelIds(out, args[1]); }
                    case "rescan", "prune", "reload", "uploads", "cache", "stats", "list", "packs", "status" -> { }
                    default -> { }
                }
            } else if (args.length == 3) {
                switch (sub) {
                    case "star" -> { out.add("add"); out.add("remove"); }
                    case "set" -> addModelIds(out, args[2]);
                    case "delete" -> out.add("--source");
                    case "packicon" -> out.add("reload");
                }
            } else if (args.length == 4 && sub.equals("star")) {
                addModelIds(out, args[3]);
            }
        }
        String prefix = args[args.length - 1].toLowerCase(Locale.ROOT);
        out.removeIf(s -> !s.toLowerCase(Locale.ROOT).startsWith(prefix));
        if (out.size() > 50) out = new ArrayList<>(out.subList(0, 50));
        return out;
    }

    private void addModelIds(List<String> out, String prefix) {
        String p = prefix == null ? "" : prefix.toLowerCase(Locale.ROOT);
        for (ModelStore.CompiledModel m : modelStore.compiledModels()) {
            if (m.modelId().toLowerCase(Locale.ROOT).startsWith(p)) {
                out.add(m.modelId());
                if (out.size() >= 50) return;
            }
        }
    }
}
