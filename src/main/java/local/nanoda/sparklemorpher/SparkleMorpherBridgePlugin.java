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
import java.util.logging.Level;

/** Folia-safe protocol bridge for Sparkle Morpher Fabric 26.1.2 clients. */
public final class SparkleMorpherBridgePlugin extends JavaPlugin implements Listener, PluginMessageListener, CommandExecutor {
    private static final int OUTBOUND_PAYLOADS_PER_REGION_RUN = 2;
    private final Map<UUID, String> negotiatedClients = new ConcurrentHashMap<>();
    private final Map<UUID, ConcurrentLinkedQueue<byte[]>> outboundPayloads = new ConcurrentHashMap<>();
    private final Map<UUID, Boolean> outboundDrains = new ConcurrentHashMap<>();
    private final Map<UUID, Set<UUID>> trackedBy = new ConcurrentHashMap<>();
    private volatile boolean allowModelUpload;
    private volatile int uploadChunkBytes;
    private volatile int uploadChunksPerTick;
    private ModelStore modelStore;
    private ModelCompiler modelCompiler;
    private LegacySyncService legacySync;
    private ExecutorService storageWorker;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        loadSettings();
        try {
            modelStore = new ModelStore(getDataFolder().toPath(), getConfig().getInt("max-model-bytes", 67_108_864),
                    getConfig().getInt("upload-timeout-seconds", 300));
        } catch (Exception error) {
            throw new IllegalStateException("Unable to initialize Sparkle model storage", error);
        }
        storageWorker = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "SparkleMorpherBridge-storage");
            thread.setDaemon(true);
            return thread;
        });
        legacySync = new LegacySyncService(modelStore, this::queuePayload);
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
        trackedBy.clear();
        if (storageWorker != null) storageWorker.shutdownNow();
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
        negotiatedClients.remove(event.getPlayer().getUniqueId());
        outboundPayloads.remove(event.getPlayer().getUniqueId());
        outboundDrains.remove(event.getPlayer().getUniqueId());
        trackedBy.remove(event.getPlayer().getUniqueId());
        trackedBy.values().forEach(receivers -> receivers.remove(event.getPlayer().getUniqueId()));
        if (legacySync != null) legacySync.remove(event.getPlayer());
    }

    @EventHandler
    public void onPlayerTrackEntity(PlayerTrackEntityEvent event) {
        if (!(event.getEntity() instanceof Player tracked)) return;
        trackedBy.computeIfAbsent(tracked.getUniqueId(), ignored -> ConcurrentHashMap.newKeySet()).add(event.getPlayer().getUniqueId());
        if (!isNegotiated(event.getPlayer())) return;
        ModelStore.Selection selection = modelStore.selection(tracked.getUniqueId());
        if (modelStore.compiled(selection.modelId()) == null) return;
        Player receiver = event.getPlayer();
        receiver.getScheduler().run(this, ignored -> sendPayload(receiver,
                SparkleWire.encodeModelState(tracked.getEntityId(), selection.modelId(), selection.textureId(),
                        toWireState(tracked.getUniqueId(), selection.modelId()))), () -> { });
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
                    sendPolicy(player);
                    legacySync.start(player);
                    sendKnownSelections(player);
                }, () -> { });
            } else if (!isNegotiated(player)) {
                return;
            } else if (message instanceof SparkleWire.LegacySyncPayload payload) {
                storageWorker.execute(() -> legacySync.accept(player, payload.data()));
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
        });
    }

    private static String userFacingError(Exception error) {
        String message = error.getMessage();
        if (message == null || message.isBlank()) return error.getClass().getSimpleName();
        return message.length() > 180 ? message.substring(0, 180) : message;
    }

    private void compileStoredModels() {
        int compiled = 0;
        int failed = 0;
        for (ModelStore.ModelFile model : modelStore.models()) {
            try {
                modelCompiler.compile(model);
                compiled++;
            } catch (Exception error) {
                failed++;
                getLogger().log(Level.WARNING, "Skipped Sparkle model " + model.fileName(), error);
            }
        }
        getLogger().info("Sparkle model scan complete: compiled=" + compiled + ", failed=" + failed + ".");
    }

    private void handleModelSelection(Player player, SparkleWire.ModelSelection selection) {
        if (modelStore.compiled(selection.modelId()) == null) return;
        modelStore.setSelection(player.getUniqueId(), selection.modelId(), selection.textureId());
        sendToTrackingAndSelf(player, SparkleWire.encodeModelState(player.getEntityId(), selection.modelId(), selection.textureId()));
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
        if (modelStore.compiled(star.modelId()) == null) return;
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
            ModelStore.Selection selection = modelStore.selection(player.getUniqueId());
            if (modelStore.compiled(selection.modelId()) == null) continue;
            sendPayload(recipient, SparkleWire.encodeModelState(player.getEntityId(), selection.modelId(), selection.textureId(),
                    toWireState(player.getUniqueId(), selection.modelId())));
        }
    }

    private SparkleWire.MolangState toWireState(UUID playerId, String modelId) {
        ModelStore.MolangState state = modelStore.molangState(playerId);
        ModelStore.CompiledModel model = modelStore.compiled(modelId);
        if (state == null || model == null || state.modelHashId() != model.modelHashId()) return null;
        return state == null ? null : new SparkleWire.MolangState(state.modelHashId(), state.values());
    }

    private void sendPayload(Player player, byte[] payload) {
        if (!player.isOnline() || !player.getListeningPluginChannels().contains(SparkleWire.CHANNEL)) return;
        player.sendPluginMessage(this, SparkleWire.CHANNEL, payload);
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
        if (outboundDrains.putIfAbsent(playerId, Boolean.TRUE) == null) {
            player.getScheduler().run(this, ignored -> drainPayloads(player), () -> clearPayloadQueue(playerId));
        }
    }

    private void drainPayloads(Player player) {
        UUID playerId = player.getUniqueId();
        ConcurrentLinkedQueue<byte[]> queue = outboundPayloads.get(playerId);
        if (queue == null || !player.isOnline()) {
            clearPayloadQueue(playerId);
            return;
        }
        for (int sent = 0; sent < OUTBOUND_PAYLOADS_PER_REGION_RUN; sent++) {
            byte[] payload = queue.poll();
            if (payload == null) break;
            sendPayload(player, payload);
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

    private void clearPayloadQueue(UUID playerId) {
        outboundPayloads.remove(playerId);
        outboundDrains.remove(playerId);
    }

    private void loadSettings() {
        reloadConfig();
        allowModelUpload = getConfig().getBoolean("allow-model-upload", false);
        uploadChunkBytes = Math.max(1024, Math.min(28_000, getConfig().getInt("upload-chunk-bytes", 24_576)));
        uploadChunksPerTick = Math.max(1, getConfig().getInt("upload-chunks-per-tick", 2));
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
