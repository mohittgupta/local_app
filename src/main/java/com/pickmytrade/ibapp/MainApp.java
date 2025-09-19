package com.pickmytrade.ibapp;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.*;
import com.google.gson.JsonObject;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.pubsub.v1.Subscription;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import com.pickmytrade.ibapp.bussinesslogic.PlaceOrderService;
import com.pickmytrade.ibapp.bussinesslogic.TwsEngine;
import com.pickmytrade.ibapp.db.DatabaseConfig;
import com.pickmytrade.ibapp.db.entities.*;
import javafx.animation.PauseTransition;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.shape.Circle;
import javafx.scene.text.Font;
import javafx.scene.text.TextAlignment;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.StageStyle;
import javafx.util.Duration;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.java_websocket.WebSocket;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.framing.Framedata;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.pickmytrade.ibapp.config.Config.log;

class UpdatableGoogleCredentials extends GoogleCredentials {
    private volatile AccessToken accessToken;

    public UpdatableGoogleCredentials(AccessToken accessToken) {
        this.accessToken = accessToken;
    }

    public void updateAccessToken(AccessToken newAccessToken) {
        this.accessToken = newAccessToken;
    }

    @Override
    public AccessToken refreshAccessToken() throws IOException {
        // Return the current (possibly updated) token. If truly expired and no update pushed, this could fail.
        return accessToken;
    }

    @Override
    public void refreshIfExpired() throws IOException {
        // Let the library check and refresh when needed
        super.refreshIfExpired();
    }
}

public class MainApp extends Application {
    public TwsEngine twsEngine;
    private PlaceOrderService placeOrderService;
    private int retrycheck_count = 1;
    private final Gson gson = new GsonBuilder().serializeNulls().create();
    private final ExecutorService executor = Executors.newFixedThreadPool(32);
    private final ExecutorService websocketExecutor = Executors.newSingleThreadExecutor();
    private ExecutorService orderExecutor = Executors.newSingleThreadExecutor();
    private WebSocketClient websocket;
    private Label twsStatusLabel;
    private Label websocketStatusLabel; // Repurposed for server connection status
    private Circle twsLight;
    private Circle websocketLight; // Repurposed for server connection status
    private TextArea consoleLog;
    private static String lastUsername = "";
    private static String lastPassword = "";
    private static String lastConnectionName = "";
    private static String lastSubscriptionId = "sub-user-A"; // Default for testing
    private TradeServer tradeServer;
    private final List<Future<?>> websocketTasks = new ArrayList<>();
    private Stage connectionStage;
    private final ReentrantLock websocketCleanupLock = new ReentrantLock();
    private final AtomicLong lastHeartbeatAck = new AtomicLong(System.currentTimeMillis());
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(16);
    private long appStartTime;
    private String app_version = "10.18.0";
    private static final String VERSION_CHECK_URL = "https://api.pickmytrade.io/v2/exe_App_latest_version";
    private static final String UPDATE_DIR = System.getenv("APPDATA") + "/PickMyTrade/updates";
    private volatile boolean isJavaFxInitialized = false;
    private volatile boolean isUpdating = false;
    private Subscriber subscriber;
    private static final AtomicBoolean pubsubIsStarting = new AtomicBoolean(false);
    private final AtomicReference<Subscriber> pubsubSubscriberRef = new AtomicReference<>();
    private final AtomicReference<UpdatableGoogleCredentials> pubsubCredentialsRef = new AtomicReference<>();
    private final AtomicLong pubsubLastMessageReceived = new AtomicLong(System.currentTimeMillis());
    private final AtomicReference<String> pubsubAccessTokenRef = new AtomicReference<>();
    private final ScheduledExecutorService pubsubScheduler = Executors.newScheduledThreadPool(1);
    private static String heartbeat_auth_token = "";
    private static String heartbeat_connection_id = "";
    private static final JsonObject SettingData = new JsonObject();

    public static void main(String[] args) {
        try {
            log.info("Checking APPDATA environment variable");
            String appDataPath = System.getenv("APPDATA");
            if (appDataPath == null) {
                log.error("APPDATA environment variable not found");
                throw new RuntimeException("APPDATA environment variable not found");
            }
            log.info("Creating PickMYTrade directory if not exists");
            File pickMyTradeDir = new File(appDataPath, "PickMYTrade");
            if (!pickMyTradeDir.exists()) {
                boolean created = pickMyTradeDir.mkdirs();
                if (!created) {
                    log.error("Failed to create directory: {}", pickMyTradeDir.getAbsolutePath());
                    throw new RuntimeException("Failed to create directory: " + pickMyTradeDir.getAbsolutePath());
                }
            }
            String logPath = pickMyTradeDir.getAbsolutePath().replace("\\", "/");
            System.setProperty("log.path", String.valueOf(pickMyTradeDir));

            log.info("Computed log directory: {}", logPath);
            log.info("System property log.path set to: {}", System.getProperty("log.path"));
            if (pickMyTradeDir.exists() && pickMyTradeDir.isDirectory()) {
                log.info("Log directory verified: {}", pickMyTradeDir.getAbsolutePath());
                if (pickMyTradeDir.canWrite()) {
                    log.info("Log directory is writable");
                } else {
                    log.error("Log directory is not writable: {}", pickMyTradeDir.getAbsolutePath());
                }
            } else {
                log.error("Log directory does not exist or is not a directory: {}", pickMyTradeDir.getAbsolutePath());
            }

            File logFile = new File(logPath, "log.log");
            try {
                if (!logFile.exists()) {
                    boolean created = logFile.createNewFile();
                    log.info("Test log file creation: {}", created ? "Created" : "Failed to create");
                } else {
                    log.info("Test log file already exists at: {}", logFile.getAbsolutePath());
                }
            } catch (IOException e) {
                log.error("Failed to create test log file: {}", e.getMessage(), e);
            }

            log.info("Starting PickMyTrade IB App");
            log.info("Loading SQLite JDBC driver");
            try {
                Class.forName("org.sqlite.JDBC");
                log.info("SQLite JDBC driver loaded successfully");
                try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
                    log.info("SQLite in-memory connection successful");
                } catch (SQLException e) {
                    log.error("SQLite connection test failed", e);
                }
            } catch (ClassNotFoundException e) {
                log.error("Failed to load SQLite JDBC driver", e);
            }

            if (logFile.exists()) {
                log.info("Log file found at: {}", logFile.getAbsolutePath());
            } else {
                log.warn("Log file not found at: {}", logFile.getAbsolutePath());
            }

            launch(args);
        } catch (Exception e) {
            log.error("Exception in main method", e);
            System.exit(1);
        }
    }

    @Override
    public void start(Stage primaryStage) {
        log.info("Entering start method");
        appStartTime = System.currentTimeMillis();
        log.info("Application started at: {}", new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(appStartTime)));
        try {
            log.info("Creating TwsEngine");
            twsEngine = new TwsEngine();
            log.info("Creating PlaceOrderService");
            placeOrderService = new PlaceOrderService(twsEngine);
            log.info("Initializing TradeServer");
            tradeServer = new TradeServer(placeOrderService);
            log.info("Starting TradeServer");
            tradeServer.start();
            log.info("Clearing OrderClient table");
            DatabaseConfig.emptyOrderClientTable();


            Platform.runLater(() -> {
                isJavaFxInitialized = true;
                log.info("JavaFX platform initialized");
            });

            log.info("Checking for updates");
            checkForUpdates(primaryStage);
        } catch (Exception e) {
            log.error("Exception in start method", e);
            throw new RuntimeException("Failed to start application", e);
        }
    }

    private void checkForUpdates(Stage primaryStage) {
        log.info("Checking for updates at: {}", VERSION_CHECK_URL);
        new Thread(() -> {
            while (!isJavaFxInitialized) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.error("Interrupted while waiting for JavaFX initialization", e);
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            try (CloseableHttpClient client = HttpClients.createDefault()) {
                HttpGet get = new HttpGet(VERSION_CHECK_URL);
                try (CloseableHttpResponse response = client.execute(get)) {
                    String responseText = EntityUtils.toString(response.getEntity());
                    log.debug("Version check response: {}", responseText);
                    Map<String, Object> versionInfo = gson.fromJson(responseText, new TypeToken<Map<String, Object>>() {
                    }.getType());
                    String minimum_version = (String) versionInfo.get("min_version");
                    String latestVersion = (String) versionInfo.get("latest_version");
                    String releaseNotes = (String) versionInfo.get("release_notes");
                    Map<String, Object> downloadUrlMap = (Map<String, Object>) versionInfo.get("download_url");

                    if (isNewerVersion(minimum_version, app_version)) {
                        log.info("New version available: {}. Current version: {}", latestVersion, app_version);
                        String downloadUrl = getPlatformDownloadUrl(downloadUrlMap, primaryStage);
                        if (downloadUrl != null) {
                            boolean proceedWithLogin = showUpdatePrompt(latestVersion, downloadUrl, releaseNotes);
                            if (proceedWithLogin) {
                                proceedToLogin(primaryStage);
                            }
                        } else {
                            log.warn("No valid download URL for the current platform");
                            Platform.runLater(() -> showErrorPopup("Update not available for this platform."));
                            proceedToLogin(primaryStage);
                        }
                    } else {
                        log.info("No new version available. Current version: {}", app_version);
                        proceedToLogin(primaryStage);
                    }
                }
            } catch (Exception e) {
                log.error("Error checking for updates: {}", e.getMessage(), e);
                Platform.runLater(() -> showErrorPopup("Failed to check for updates: " + e.getMessage()));
                proceedToLogin(primaryStage);
            }
        }).start();
    }

    private void proceedToLogin(Stage primaryStage) {
        Platform.runLater(() -> {
            log.info("No update or update skipped, proceeding with UI");
            log.info("Showing splash screen");
            showSplashScreen(primaryStage);
            log.info("Creating login stage");
            Stage loginStage = new Stage();
            log.info("Showing login window");
            showLoginWindow(loginStage);
        });
    }

    private boolean isNewerVersion(String latestVersion, String currentVersion) {
        String[] latestParts = latestVersion.split("\\.");
        String[] currentParts = currentVersion.split("\\.");
        int length = Math.max(latestParts.length, currentParts.length);

        for (int i = 0; i < length; i++) {
            int latestNum = i < latestParts.length ? Integer.parseInt(latestParts[i]) : 0;
            int currentNum = i < currentParts.length ? Integer.parseInt(currentParts[i]) : 0;
            if (latestNum > currentNum) {
                return true;
            } else if (latestNum < currentNum) {
                return false;
            }
        }
        return false;
    }

    private String getPlatformDownloadUrl(Map<String, Object> downloadUrlMap, Stage primaryStage) {
        String osName = System.getProperty("os.name").toLowerCase();
        log.debug("Detected OS: {}", osName);

        if (osName.contains("win")) {
            String windowsUrl = (String) downloadUrlMap.get("windows");
            if (windowsUrl == null || windowsUrl.isEmpty()) {
                log.warn("No Windows download URL provided in server response");
                return null;
            }
            log.info("Using Windows download URL: {}", windowsUrl);
            return windowsUrl;
        } else if (osName.contains("mac")) {
            Map<String, String> macUrls = (Map<String, String>) downloadUrlMap.get("mac");
            if (macUrls == null || macUrls.isEmpty()) {
                log.warn("No macOS download URLs provided in server response");
                return null;
            }
            return showMacOsSelectionPopup(macUrls, primaryStage);
        } else {
            log.warn("Unsupported platform: {}", osName);
            return null;
        }
    }

    private String showMacOsSelectionPopup(Map<String, String> macUrls, Stage primaryStage) {
        log.info("Showing macOS architecture selection popup");
        Stage popup = new Stage();
        popup.initModality(Modality.APPLICATION_MODAL);
        popup.initOwner(primaryStage);
        popup.setTitle("Select macOS Architecture");

        VBox layout = new VBox(10);
        layout.setAlignment(Pos.CENTER);
        layout.setPadding(new Insets(20));

        Label label = new Label("Please select your macOS architecture for the update:");
        label.setFont(Font.font("Arial", 14));

        ComboBox<String> architectureCombo = new ComboBox<>();
        architectureCombo.getItems().addAll("Silicon (Apple M1/M2)", "Intel");
        architectureCombo.setValue("Silicon (Apple M1/M2)");
        architectureCombo.setStyle("-fx-font-size: 14; -fx-pref-height: 40; -fx-border-radius: 20");

        Button okButton = new Button("OK");
        okButton.setStyle("-fx-background-color: #dc143c; -fx-text-fill: white; -fx-border-radius: 20; -fx-pref-height: 40");
        okButton.setOnAction(e -> popup.close());

        layout.getChildren().addAll(label, architectureCombo, okButton);
        Scene scene = new Scene(layout, 400, 200);
        popup.setScene(scene);

        popup.showAndWait();
        String selected = architectureCombo.getValue();
        String downloadUrl = selected.equals("Silicon (Apple M1/M2)") ? macUrls.get("silicon") : macUrls.get("intel");
        log.info("Selected macOS architecture: {}, URL: {}", selected, downloadUrl != null ? downloadUrl : "None");
        return downloadUrl;
    }

    private boolean showUpdatePrompt(String latestVersion, String downloadUrl, String releaseNotes) {
        log.info("Showing update prompt for version: {}", latestVersion);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean proceedWithLogin = new AtomicBoolean(true);

        Platform.runLater(() -> {
            try {
                log.debug("Creating update alert dialog");
                Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
                alert.setTitle("Update Available");
                alert.setHeaderText("A new version (" + latestVersion + ") is available!");
                alert.setContentText("Release Notes: " + releaseNotes + "\nDownloading and Installing Latest version of PickMyTrade IB App.");

                isUpdating = true;
                executor.submit(() -> {
                    try {
                        downloadAndInstallUpdate(downloadUrl, latestVersion);
                    } finally {
                        isUpdating = false;
                    }
                });
                proceedWithLogin.set(false);

            } catch (Exception e) {
                log.error("Error showing update prompt: {}", e.getMessage(), e);
                Platform.runLater(() -> showErrorPopup("Failed to show update prompt: " + e.getMessage()));
                proceedWithLogin.set(true);
            } finally {
                latch.countDown();
            }
        });

        try {
            boolean completed = latch.await(20, TimeUnit.SECONDS);
            if (!completed) {
                log.error("Update prompt dialog timed out after 20 seconds");
                Platform.runLater(() -> showErrorPopup("Update prompt failed to respond. Proceeding to login."));
                proceedWithLogin.set(true);
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for update prompt: {}", e.getMessage());
            Thread.currentThread().interrupt();
            proceedWithLogin.set(true);
        }

        return proceedWithLogin.get();
    }

    private void downloadAndInstallUpdate(String downloadUrl, String version) {
        log.info("Downloading update from: {}", downloadUrl);
        try {
            Path updateDir = Paths.get(UPDATE_DIR);
            Files.createDirectories(updateDir);
            String fileExtension = System.getProperty("os.name").toLowerCase().contains("win") ? ".msi" : ".pkg";
            Path installerPath = updateDir.resolve("PickMyTrade-IB-" + version + fileExtension);

            final Stage[] progressStage = new Stage[1];
            final ProgressBar[] progressBar = new ProgressBar[1];
            final Label[] progressLabel = new Label[1];
            Platform.runLater(() -> {
                log.debug("Creating progress dialog on JavaFX thread");
                progressStage[0] = new Stage();
                progressStage[0].initModality(Modality.APPLICATION_MODAL);
                progressStage[0].setTitle("Downloading Update v" + version);
                progressStage[0].setAlwaysOnTop(true);
                progressStage[0].centerOnScreen();
                VBox layout = new VBox(10);
                layout.setAlignment(Pos.CENTER);
                layout.setPadding(new Insets(20));
                Label label = new Label("Downloading update, please wait...");
                label.setFont(Font.font("Arial", 14));
                progressBar[0] = new ProgressBar();
                progressBar[0].setPrefWidth(300);
                progressBar[0].setProgress(ProgressBar.INDETERMINATE_PROGRESS);
                progressLabel[0] = new Label("Initializing...");
                progressLabel[0].setFont(Font.font("Arial", 12));
                layout.getChildren().addAll(label, progressBar[0], progressLabel[0]);
                Scene scene = new Scene(layout, 350, 200);
                progressStage[0].setScene(scene);
                progressStage[0].show();
                log.debug("Progress dialog shown, isShowing: {}", progressStage[0].isShowing());
            });

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for progress dialog: {}", e.getMessage());
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during update process");
            }

            log.info("Downloading installer to: {}", installerPath);
            long totalBytes = 0;
            final long[] contentLengthHolder = new long[]{-1};
            try {
                URL url = new URL(downloadUrl);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("HEAD");
                contentLengthHolder[0] = connection.getContentLengthLong();
                connection.disconnect();
                log.info("Content-Length from server: {} bytes", contentLengthHolder[0] >= 0 ? contentLengthHolder[0] : "unknown");

                Platform.runLater(() -> {
                    log.debug("Setting initial progress bar state on JavaFX thread");
                    if (contentLengthHolder[0] > 0) {
                        progressBar[0].setProgress(0);
                        progressLabel[0].setText(String.format("0%% (0/%d bytes)", contentLengthHolder[0]));
                    } else {
                        progressBar[0].setProgress(ProgressBar.INDETERMINATE_PROGRESS);
                        progressLabel[0].setText("Unknown size, starting download...");
                    }
                });

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.error("Interrupted while waiting for initial UI update: {}", e.getMessage());
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during update process");
                }

                try (InputStream in = url.openStream();
                     FileOutputStream out = new FileOutputStream(installerPath.toFile())) {
                    byte[] buffer = new byte[1024];
                    int bytesRead;
                    long lastUpdateTime = System.currentTimeMillis();
                    final long[] updateInterval = new long[]{50};
                    while ((bytesRead = in.read(buffer)) != -1) {
                        out.write(buffer, 0, bytesRead);
                        totalBytes += bytesRead;
                        final long downloaded = totalBytes;
                        if (System.currentTimeMillis() - lastUpdateTime >= updateInterval[0]) {
                            Platform.runLater(() -> {
                                log.debug("Updating progress: {} bytes downloaded on JavaFX thread", downloaded);
                                if (contentLengthHolder[0] > 0) {
                                    double progress = (double) downloaded / contentLengthHolder[0];
                                    progressBar[0].setProgress(Math.min(progress, 1.0));
                                    progressLabel[0].setText(String.format("%.1f%% (%d/%d bytes)", progress * 100, downloaded, contentLengthHolder[0]));
                                } else {
                                    progressLabel[0].setText(String.format("%d bytes downloaded", downloaded));
                                }
                            });
                            lastUpdateTime = System.currentTimeMillis();
                        }
                    }
                    log.info("Update downloaded successfully, size: {} bytes", totalBytes);
                }
            } catch (IOException e) {
                log.error("Failed to download installer: {}", e.getMessage(), e);
                throw new IOException("Failed to download installer", e);
            }

            final long finalTotalBytes = totalBytes;
            Platform.runLater(() -> {
                log.debug("Finalizing progress bar state");
                if (contentLengthHolder[0] > 0) {
                    progressBar[0].setProgress(1.0);
                    progressLabel[0].setText(String.format("100%% (%d/%d bytes)", finalTotalBytes, contentLengthHolder[0]));
                } else {
                    progressLabel[0].setText(String.format("%d bytes downloaded", finalTotalBytes));
                }
            });

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted while showing final progress: {}", e.getMessage());
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during update process");
            }

            Platform.runLater(() -> {
                log.debug("Closing progress dialog");
                if (progressStage[0] != null) {
                    progressStage[0].close();
                }
            });

            File installerFile = installerPath.toFile();
            if (!installerFile.exists() || installerFile.length() == 0) {
                log.error("Installer file does not exist or is empty: {}", installerPath);
                throw new IOException("Downloaded installer file is invalid or empty");
            }
            if (contentLengthHolder[0] > 0 && installerFile.length() != contentLengthHolder[0]) {
                log.error("Installer file size mismatch. Expected: {} bytes, Actual: {} bytes", contentLengthHolder[0], installerFile.length());
                throw new IOException("Downloaded installer file size does not match expected size");
            }
            if (!installerFile.canExecute()) {
                log.warn("Installer file is not executable, attempting to set executable permission: {}", installerPath);
                boolean setExecutable = installerFile.setExecutable(true);
                if (!setExecutable) {
                    log.error("Failed to set executable permission for installer: {}", installerPath);
                    throw new IOException("Cannot set executable permission for installer");
                }
            }

            app_version = version;
            log.info("Updated app_version to: {}", version);

            log.info("Launching installer: {}", installerPath);
            ProcessBuilder pb;
            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                pb = new ProcessBuilder("cmd.exe", "/c", "start", "\"\"", "\"" + installerPath.toString() + "\"");
            } else {
                pb = new ProcessBuilder("open", "-W", installerPath.toString());
            }
            pb.redirectErrorStream(true);
            pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
            pb.redirectError(ProcessBuilder.Redirect.PIPE);

            Process process = pb.start();
            shutdownApplication();
            StringBuilder processOutput = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    processOutput.append(line).append("\n");
                }
            }
            String output = processOutput.toString();
            log.info("Installer process output: {}", output.isEmpty() ? "<empty>" : output);
            shutdownApplication();
            try {
                boolean exited = process.waitFor(5, TimeUnit.SECONDS);
                if (exited) {
                    int exitCode = process.exitValue();
                    isUpdating = false;
                    shutdownApplication();
                } else {
                    log.info("Installer process started successfully, continuing");
                    isUpdating = false;
                    shutdownApplication();
                }
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for installer to start: {}", e.getMessage());
                isUpdating = false;
                shutdownApplication();
            }

        } catch (Exception e) {
            log.error("Error downloading or installing update: {}", e.getMessage(), e);
            isUpdating = false;
            shutdownApplication();
        }
    }

    @Override
    public void stop() {
        if (isUpdating) {
            log.info("Application stop requested but update is in progress, delaying shutdown");
            return;
        }
        log.info("Application stop method called");
        new Thread(() -> {
            try {
                showCloseWarningPopup();
                shutdownApplication();
            } catch (Exception e) {
                log.error("Error during application stop: {}", e.getMessage());
                System.exit(1);
            }
        }).start();
    }

    private void showCloseWarningPopup() {
        log.info("Displaying application closing warning popup");
        CountDownLatch latch = new CountDownLatch(1);

        Platform.runLater(() -> {
            try {
                Stage popupStage = new Stage();
                popupStage.initModality(Modality.APPLICATION_MODAL);
                popupStage.initStyle(StageStyle.UTILITY);
                popupStage.setTitle("PickMyTrade IB App Closing");

                VBox layout = new VBox(20);
                layout.setAlignment(Pos.CENTER);
                layout.setPadding(new Insets(20));
                layout.setStyle("-fx-background-color: white;");

                Label title = new Label("PickMyTrade IB App Closing");
                title.setFont(Font.font("Arial", 18));
                title.setTextFill(Color.BLACK);
                title.setTextAlignment(TextAlignment.CENTER);

                Label message = new Label("The application is closing. Please manage your open positions and open orders manually.");
                message.setFont(Font.font("Arial", 14));
                message.setWrapText(true);
                message.setTextAlignment(TextAlignment.CENTER);
                message.setTextFill(Color.BLACK);

                Button okButton = new Button("OK");
                okButton.setPrefHeight(40);
                okButton.setStyle("-fx-background-color: #dc143c; -fx-text-fill: white; -fx-border-radius: 20; -fx-font-family: Arial; -fx-font-size: 14;");
                okButton.setOnMouseEntered(e -> okButton.setStyle("-fx-background-color: #c21032; -fx-text-fill: white; -fx-border-radius: 20; -fx-font-family: Arial; -fx-font-size: 14;"));
                okButton.setOnMouseExited(e -> okButton.setStyle("-fx-background-color: #dc143c; -fx-text-fill: white; -fx-border-radius: 20; -fx-font-family: Arial; -fx-font-size: 14;"));
                okButton.setOnAction(e -> {
                    log.debug("User clicked OK on warning popup");
                    popupStage.close();
                    latch.countDown();
                });

                layout.getChildren().addAll(title, message, okButton);
                Scene scene = new Scene(layout, 400, 250);
                popupStage.setScene(scene);

                PauseTransition delay = new PauseTransition(Duration.seconds(5));
                delay.setOnFinished(e -> {
                    if (popupStage.isShowing()) {
                        log.debug("Auto-closing warning popup");
                        popupStage.close();
                        latch.countDown();
                    }
                });

                log.debug("Showing warning popup");
                popupStage.show();
                delay.play();
            } catch (Exception e) {
                log.error("Error displaying warning popup: {}", e.getMessage(), e);
                latch.countDown();
            }
        });

        try {
            boolean completed = latch.await(6, TimeUnit.SECONDS);
            if (completed) {
                log.info("Application closing warning popup displayed and closed");
            } else {
                log.warn("Popup did not close within 6 seconds, proceeding with shutdown");
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for warning popup: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void shutdownApplication() {
        log.info("Shutting down application...");
        long runtimeMillis = System.currentTimeMillis() - appStartTime;
        log.info("Application ran for {} seconds", runtimeMillis / 1000);
        try {
            synchronized (websocketTasks) {
                websocketTasks.forEach(task -> task.cancel(true));
                websocketTasks.clear();
            }

            if (tradeServer != null) {
                log.info("Stopping trade server...");
                tradeServer.stop();
            }

            if (twsEngine != null && twsEngine.isConnected()) {
                log.info("Disconnecting TWS...");
                twsEngine.disconnect();
            }

            if (websocket != null && websocket.isOpen()) {
                log.info("Closing WebSocket...");
                websocket.close();
            }

            if (subscriber != null) {
                log.info("Stopping server connection subscriber...");
                try {
                    subscriber.stopAsync().awaitTerminated(5, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    log.warn("server connection subscriber did not terminate in time, forcing shutdown...");
                }
                subscriber = null;
            }

            if (executor != null && !executor.isShutdown()) {
                log.info("Shutting down executor service...");
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.warn("Executor did not terminate in time, forcing shutdown...");
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    log.error("Executor shutdown interrupted: {}", e.getMessage());
                    executor.shutdownNow();
                }
            }

            if (websocketExecutor != null && !websocketExecutor.isShutdown()) {
                log.info("Shutting down WebSocket executor service...");
                websocketExecutor.shutdown();
                try {
                    if (!websocketExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.warn("WebSocket executor did not terminate in time, forcing shutdown...");
                        websocketExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    log.error("WebSocket executor shutdown interrupted: {}", e.getMessage());
                    websocketExecutor.shutdownNow();
                }
            }

            log.info("Exiting JavaFX platform...");
            Platform.exit();

            log.info("Terminating JVM...");
            System.exit(0);
        } catch (Exception e) {
            log.error("Error during shutdown: {}", e.getMessage());
            System.exit(1);
        }
    }

    private void showSplashScreen(Stage primaryStage) {
        log.info("Displaying splash screen");
        try {
            Stage splashStage = new Stage();
            splashStage.initOwner(primaryStage);
            splashStage.initStyle(StageStyle.UNDECORATED);
            VBox splashLayout = new VBox(20);
            splashLayout.setAlignment(Pos.CENTER);
            splashLayout.setStyle("-fx-background-color: white; -fx-padding: 20;");
            log.info("Loading spinner.gif");
            Image spinnerImage = new Image(getClass().getResourceAsStream("/spinner.gif"));
            if (spinnerImage.isError()) {
                log.error("Failed to load spinner.gif");
                throw new RuntimeException("Missing or invalid spinner.gif");
            }
            ImageView spinner = new ImageView(spinnerImage);
            spinner.setFitWidth(100);
            spinner.setFitHeight(100);
            Label text = new Label("PickMyTrade IB App starting");
            text.setFont(Font.font("Arial", 18));
            text.setTextFill(Color.BLACK);
            splashLayout.getChildren().addAll(spinner, text);
            Scene splashScene = new Scene(splashLayout, 400, 400);
            splashStage.setScene(splashScene);
            splashStage.setOnCloseRequest(e -> {
                if (isUpdating) {
                    log.info("Splash screen close request ignored due to ongoing update");
                    e.consume();
                }
            });
            splashStage.show();

            new Thread(() -> {
                try {
                    log.debug("Splash screen displayed, waiting for 5 seconds");
                    Thread.sleep(5000);
                    Platform.runLater(() -> {
                        log.info("Closing splash screen");
                        splashStage.close();
                    });
                } catch (InterruptedException e) {
                    log.error("Splash screen interrupted: {}", e.getMessage());
                }
            }).start();
        } catch (Exception e) {
            log.error("Error in showSplashScreen", e);
            throw e;
        }
    }

    private void showLoginWindow(Stage loginStage) {
        log.info("Setting up login window");
        try {
            loginStage.setTitle("Login");
            VBox layout = new VBox(15);
            layout.setPadding(new Insets(20));
            layout.setStyle("-fx-background-color: white;");
            layout.setAlignment(Pos.TOP_CENTER);

            // Title with logo
            HBox titleLayout = new HBox(10);
            titleLayout.setAlignment(Pos.CENTER);
            log.info("Loading logo.png");
            Image logoImage = new Image(getClass().getResourceAsStream("/logo.png"));
            if (logoImage.isError()) {
                log.error("Failed to load logo.png");
                throw new RuntimeException("Missing or invalid logo.png");
            }
            ImageView logo = new ImageView(logoImage);
            logo.setFitWidth(120);
            logo.setFitHeight(120);
            Label title = new Label("PickMyTrade IB application");
            title.setFont(Font.font("Arial", 18));
            titleLayout.getChildren().addAll(logo, title);

            // Subtitle
            Label subtitle = new Label("Please ensure TWS is open and logged in before accessing this application.");
            subtitle.setFont(Font.font("Arial", 14));
            subtitle.setWrapText(true);
            subtitle.setTextAlignment(TextAlignment.CENTER);

            // Email input
            Label emailLabel = new Label("Email*");
            TextField emailInput = new TextField();
            emailInput.setPromptText("Enter Email");
            emailInput.setPrefHeight(40);
            emailInput.setStyle("-fx-border-radius: 20;");

            // Password input
            Label passwordLabel = new Label("Password*");
            PasswordField passwordInput = new PasswordField();
            passwordInput.setPromptText("Enter your password");
            passwordInput.setPrefHeight(40);
            passwordInput.setStyle("-fx-border-radius: 20;");

            // Login button
            Button loginButton = new Button("Login");
            loginButton.setPrefHeight(50);
            loginButton.setStyle("-fx-background-color: #dc143c; -fx-text-fill: white; -fx-border-radius: 20;");
            loginButton.setOnMouseEntered(e -> loginButton.setStyle("-fx-background-color: #c21032; -fx-text-fill: white; -fx-border-radius: 20;"));
            loginButton.setOnMouseExited(e -> loginButton.setStyle("-fx-background-color: #dc143c; -fx-text-fill: white; -fx-border-radius: 20;"));

            // Loader
            ProgressBar loader = new ProgressBar();
            loader.setPrefHeight(20);
            loader.setVisible(false);
            loader.setStyle("-fx-accent: #dc143c;");

            // Error label
            Label errorLabel = new Label("");
            errorLabel.setTextFill(Color.RED);
            errorLabel.setFont(Font.font("Arial", 10));
            errorLabel.setWrapText(true);
            errorLabel.setTextAlignment(TextAlignment.CENTER);

            // Actions
            loginButton.setOnAction(e -> login(loginStage, emailInput.getText(), passwordInput.getText(), errorLabel, loader, loginButton));
            passwordInput.setOnAction(e -> login(loginStage, emailInput.getText(), passwordInput.getText(), errorLabel, loader, loginButton));

            // New buttons
            Button videosButton = new Button("View Tutorials");
            videosButton.setPrefHeight(40);
            videosButton.setStyle("-fx-background-color: #007bff; -fx-text-fill: white; -fx-border-radius: 20;");
            videosButton.setOnAction(e -> showVideosPopup());

            Button sendLogsButton = new Button("Reset Connection");
            sendLogsButton.setPrefHeight(40);
            sendLogsButton.setStyle("-fx-background-color: #6c757d; -fx-text-fill: white; -fx-border-radius: 20;");
            sendLogsButton.setOnAction(e -> executor.submit(this::resetconnection));

            HBox buttonsHBox = new HBox(10);
            buttonsHBox.setAlignment(Pos.CENTER);
            buttonsHBox.getChildren().addAll(videosButton, sendLogsButton);

            // Spacer to push bottom buttons down
            Region spacer = new Region();
            VBox.setVgrow(spacer, Priority.ALWAYS);

            // Add everything to layout
            layout.getChildren().addAll(
                    titleLayout,
                    subtitle,
                    emailLabel, emailInput,
                    passwordLabel, passwordInput,
                    loginButton,
                    loader,
                    errorLabel,
                    spacer,          // pushes buttonsHBox to bottom
                    buttonsHBox
            );

            // Scene
            Scene scene = new Scene(layout, 600, 500);
            loginStage.setScene(scene);
            loginStage.setOnCloseRequest(e -> {
                if (isUpdating) {
                    log.info("Login window close request ignored due to ongoing update");
                    e.consume();
                } else {
                    log.info("Close request received for login stage. Initiating shutdown.");
                    stop();
                }
            });
            loginStage.show();

            if (!lastUsername.isEmpty()) {
                log.debug("Populating email input with last username: {}", lastUsername);
                emailInput.setText(lastUsername);
            }
            if (!lastPassword.isEmpty()) {
                log.debug("Populating password input with last password");
                passwordInput.setText(lastPassword);
            }
        } catch (Exception e) {
            log.error("Error in showLoginWindow", e);
            throw e;
        }
    }


    private void showVideosPopup() {
        Stage popup = new Stage();
        popup.initModality(Modality.APPLICATION_MODAL);
        popup.setTitle("Tutorial Videos");

        VBox layout = new VBox(10);
        layout.setPadding(new Insets(20));
        layout.setStyle("-fx-background-color: white;");

        List<Map<String, String>> videos = Arrays.asList(
                Map.of("title", "How to set up automated trading from TradingView to Interactive Brokers?",
                        "link", "https://youtu.be/7P7tEw0zSIQ"),
                Map.of("title", "Automating TradingView strategies on Interactive Brokers for futures and stocks.",
                        "link", "https://youtu.be/iwhUZKdBCzI"),
                Map.of("title", "Automating TradingView indicators with Interactive Brokers (IBKR).",
                        "link", "https://youtu.be/U1aM18NXk10"),
                Map.of("title", "How to Automate Interactive Brokers (IBKR) Trading Using a TradingView Indicator",
                        "link", "https://youtu.be/TAZuCYmYp9A"),
                Map.of("title", "How to Automate Interactive Brokers (IBKR) Trading Using a TradingView Strategy",
                        "link", "https://youtu.be/TfqrekJ6SZw")
        );

        for (Map<String, String> video : videos) {
            HBox videoBox = new HBox(10);
            videoBox.setAlignment(Pos.CENTER_LEFT);

            // Extract videoId from link
            String link = video.get("link");
            String videoId = null;
            if (link.contains("youtu.be/")) {
                videoId = link.substring(link.lastIndexOf("/") + 1);
            } else if (link.contains("v=")) {
                videoId = link.substring(link.indexOf("v=") + 2);
                int ampIndex = videoId.indexOf("&");
                if (ampIndex != -1) {
                    videoId = videoId.substring(0, ampIndex);
                }
            }

            String thumbnailUrl = "https://img.youtube.com/vi/" + videoId + "/0.jpg";
            ImageView thumbnail = new ImageView(new Image(thumbnailUrl, true));
            thumbnail.setFitWidth(120);
            thumbnail.setFitHeight(90);

            Label titleLabel = new Label(video.get("title"));
            titleLabel.setFont(Font.font("Arial", 14));
            titleLabel.setWrapText(true);

            videoBox.getChildren().addAll(thumbnail, titleLabel);
            videoBox.setOnMouseClicked(e -> getHostServices().showDocument(link));
            videoBox.setStyle("-fx-cursor: hand;");

            layout.getChildren().add(videoBox);
        }

        ScrollPane scrollPane = new ScrollPane(layout);
        scrollPane.setFitToWidth(true);
        scrollPane.setStyle("-fx-background-color: white;");

        Scene scene = new Scene(scrollPane, 600, 400);
        popup.setScene(scene);
        popup.showAndWait();
    }


    private void login(Stage loginStage, String username, String password, Label errorLabel, ProgressBar loader, Button loginButton) {
        log.info("Attempting login for user: {}", username);
        String termsMessage = """
                PickMyTrade offers a powerful solution for fully automating trade execution through webhooks that link directly to your broker and exchange accounts. However, it’s important to recognize that automation carries inherent risks. Whether you're a beginner or a seasoned trader, it’s vital to approach automated trading carefully and stay mindful of the potential dangers involved.
                
                BY USING PICKMYTRADE, YOU ACKNOWLEDGE THAT YOU ARE AWARE OF THE RISKS ASSOCIATED WITH AUTOMATING TRADES ON THE PLATFORM AND ACCEPT FULL RESPONSIBILITY FOR THESE RISKS.
                
                By continuing to use PickMyTrade, you agree to release the company, its parent entity, affiliates, and employees from any claims, liabilities, costs, losses, damages, or expenses resulting from or related to your use of the platform. This includes, but is not limited to, any complications caused by system errors, malfunctions, or downtime impacting PickMyTrade or its third-party service providers. You are fully responsible for monitoring your trades and ensuring that your signals are accurately executed. By using PickMyTrade, you accept this waiver and confirm that you have read, understood, and agreed to the PickMyTrade Terms of Service and Privacy Policy, which includes additional disclaimers and restrictions.
                """;
        try {
            log.debug("Checking existing token in database");
            Token token = DatabaseConfig.getToken();
            if (token == null) {
                log.info("No existing token found, showing terms confirmation");
                Alert alert = new Alert(Alert.AlertType.CONFIRMATION, termsMessage, ButtonType.YES, ButtonType.NO);
                alert.setTitle("Connect");
                alert.setHeaderText(null);
                Optional<ButtonType> result = alert.showAndWait();
                if (result.isEmpty() || result.get() != ButtonType.YES) {
                    log.info("User declined terms, aborting login");
                    return;
                }
            }

            log.debug("Showing loader and disabling login button");
            loader.setVisible(true);
            loginButton.setDisable(true);

            Map<String, String> payload = new HashMap<>();
            payload.put("username", username);
            payload.put("password", password);
            payload.put("app_version", app_version );
            ConnectionEntity connection = DatabaseConfig.getConnectionEntity();
            if (connection != null) {
                log.debug("Adding connection name to payload: {}", connection.getConnectionName());
                payload.put("connection_name", connection.getConnectionName());
            }

            log.info("Sending login request with payload: {}", gson.toJson(payload));
            Map<String, Object> response = loginAndGetToken(payload);
            log.info("Login response: {}", gson.toJson(response));

            log.debug("Hiding loader and enabling login button");
            loader.setVisible(false);
            loginButton.setDisable(false);

            if (response.containsKey("success") && (boolean) response.get("success")) {
                log.info("Login successful for user: {}", username);
                lastUsername = username;
                lastPassword = password;
                lastConnectionName = (String) response.get("connection_name");
                lastSubscriptionId = response.containsKey("subscription_id") ? (String) response.get("subscription_id") : "sub-user-A";
                String accessTokenString = response.containsKey("access_token") ? (String) response.get("access_token") : "ya29.c.c0ASRK0....."; // Fallback to default
                log.info("Subscription ID: {}, Access Token: {}", lastSubscriptionId, accessTokenString);

                connectionStage = new Stage();
                connectionStage.setTitle("Connection Status");
                connectionStage.setScene(createConnectionStatusScene(connectionStage));
                connectionStage.setOnCloseRequest(e -> {
                    if (isUpdating) {
                        log.info("Connection stage close request ignored due to ongoing update");
                        e.consume();
                    } else {
                        log.info("Close request received for connection stage. Initiating shutdown.");
                        stop();
                    }
                });
                log.info("Showing connection status window");
                connectionStage.show();
                log.debug("Closing login stage");
                loginStage.close();
                log.info("Starting TWS and server connection connections after successful login");
                heartbeat_auth_token = (String) response.get("connection_name");
                heartbeat_connection_id = (String) response.get("id");
                orderExecutor.submit(this::scheduleOrderSender);
//                executor.submit(() -> monitorHeartbeatAck(connectionStage));
                executor.submit(() -> continuouslyCheckTwsConnection(connectionStage));
                // Comment out WebSocket initialization as per requirement
                // websocketExecutor.submit(() -> checkWebsocket(connectionStage));

                // Start server connection subscriber with access token
                String finalAccessTokenString = accessTokenString;
                executor.submit(() -> startPubSubSubscriber(lastSubscriptionId, finalAccessTokenString));
                executor.submit(() -> monitorPubSubConnection(lastSubscriptionId));
                executor.submit(this::sendAccountDataToServer);
            } else if (response.containsKey("connections")) {
                log.info("Multiple connections available, showing connection selection");
                List<String> connections = (List<String>) response.get("connections");
                if (!connections.isEmpty()) {
                    String selectedConnection = showConnectionPopup(connections);
                    if (selectedConnection != null) {
                        log.info("User selected connection: {}", selectedConnection);
                        payload.put("connection_name", selectedConnection);
                        log.info("Retrying login with selected connection, payload: {}", gson.toJson(payload));
                        response = loginAndGetToken(payload);
                        log.info("Login response after connection selection: {}", gson.toJson(response));
                        if (response.containsKey("success") && (boolean) response.get("success")) {
                            log.info("Login successful with selected connection: {}", selectedConnection);
                            lastUsername = username;
                            lastPassword = password;
                            lastConnectionName = selectedConnection;
                            lastSubscriptionId = response.containsKey("subscription_id") ? (String) response.get("subscription_id") : "sub-user-A";
                            String accessTokenString = response.containsKey("access_token") ? (String) response.get("access_token") : "ya29.c.c0ASRK0....."; // Fallback to default
                            log.info("Subscription ID: {}, Access Token: {}", lastSubscriptionId, accessTokenString);

                            connectionStage = new Stage();
                            connectionStage.setTitle("Connection Status");
                            connectionStage.setScene(createConnectionStatusScene(connectionStage));
                            connectionStage.setOnCloseRequest(e -> {
                                if (isUpdating) {
                                    log.info("Connection stage close request ignored due to ongoing update");
                                    e.consume();
                                } else {
                                    log.info("Close request received for connection stage. Initiating shutdown.");
                                    stop();
                                }
                            });
                            log.info("Showing connection status window");
                            connectionStage.show();
                            log.debug("Closing login stage");
                            loginStage.close();
                            log.info("Starting TWS and server connection connections after successful login");
                            orderExecutor.submit(this::scheduleOrderSender);
//                            executor.submit(() -> monitorHeartbeatAck(connectionStage));
                            executor.submit(() -> continuouslyCheckTwsConnection(connectionStage));
                            // Comment out WebSocket initialization as per requirement
                            // websocketExecutor.submit(() -> checkWebsocket(connectionStage));

                            // Start server connection subscriber with access token
                            heartbeat_auth_token = (String) response.get("connection_name");
                            heartbeat_connection_id = (String) response.get("id");
                            String finalAccessTokenString = accessTokenString;
                            executor.submit(() -> startPubSubSubscriber(lastSubscriptionId, finalAccessTokenString));
                            executor.submit(() -> monitorPubSubConnection(lastSubscriptionId));
                            executor.submit(this::sendAccountDataToServer);

                        } else {
                            String errorMessage = response.getOrDefault("message", response.getOrDefault("data", "Login failed")).toString();
                            log.warn("Login failed after connection selection: {}", errorMessage);
                            showErrorPopup(errorMessage);
                        }
                    } else {
                        log.info("Connection selection cancelled by user");
                        showErrorPopup("Connection selection cancelled");
                    }
                } else {
                    String errorMessage = response.getOrDefault("message", response.getOrDefault("data", "No connections available")).toString();
                    log.warn("Login failed, no connections available: {}", errorMessage);
                    showErrorPopup(errorMessage);
                }
            } else {
                String errorMessage = response.getOrDefault("message", response.getOrDefault("data", "Login failed")).toString();
                log.warn("Login failed: {}", errorMessage);
                showErrorPopup(errorMessage);
            }
        } catch (Exception e) {
            String errorMessage = e.getMessage() != null ? e.getMessage() : "An unexpected error occurred during login";
            log.error("Login error: {}", errorMessage, e);
            showErrorPopup(errorMessage);
        }
    }

    // Class-level fields


    private boolean validateSubscription(String projectId, String subscriptionId, GoogleCredentials credentials) {
        try {
            SubscriptionAdminSettings adminSettings =
                    SubscriptionAdminSettings.newBuilder()
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
                            .build();

            try (SubscriptionAdminClient adminClient = SubscriptionAdminClient.create(adminSettings)) {
                ProjectSubscriptionName subName = ProjectSubscriptionName.of(projectId, subscriptionId);
                Subscription sub = adminClient.getSubscription(subName);
                log.info("✅ Subscription exists: {}", sub.getName());
                return true;
            }
        } catch (Exception e) {
            log.error("❌ Subscription validation failed: {}", e.getMessage(), e);
            return false;
        }
    }

    private String refreshAccessToken(String currentAccessToken) {
        log.info("Attempting to refresh access token via exe/pubsubtoken");
        Map<String, String> payload = new HashMap<>();
        payload.put("username", lastUsername);
        payload.put("password", lastPassword);
        payload.put("app_version", app_version);
        if (!lastConnectionName.isEmpty()) {
            payload.put("connection_name", lastConnectionName);
        }

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost("https://api.pickmytrade.io/v2/refesh_pubsubtoken");
            post.setEntity(new StringEntity(gson.toJson(payload)));
            post.setHeader("Content-Type", "application/json");
            log.debug("Executing HTTP POST request to exe/pubsubtoken");

            try (CloseableHttpResponse response = client.execute(post)) {
                String responseText = EntityUtils.toString(response.getEntity());
                log.info("Received response from exe/pubsubtoken: {}", responseText);

                Map<String, Object> responseMap;
                try {
                    if (responseText.trim().startsWith("{")) {
                        responseMap = gson.fromJson(responseText, new TypeToken<Map<String, Object>>() {
                        }.getType());
                    } else {
                        log.warn("exe/pubsubtoken API returned a string instead of a JSON object: {}", responseText);
                        return null;
                    }
                } catch (Exception parseEx) {
                    log.error("Failed to parse exe/pubsubtoken response as JSON: {}. Raw response: {}", parseEx.getMessage(), responseText);
                    return null;
                }

                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200 && responseMap.containsKey("access_token")) {
                    String newAccessToken = (String) responseMap.get("access_token");
                    log.info("Successfully refreshed access token via exe/pubsubtoken");
                    return newAccessToken;
                } else {
                    String errorMessage = responseMap.getOrDefault("message", responseMap.getOrDefault("data", "Failed to refresh access token")).toString();
                    log.error("Failed to refresh access token: {}", errorMessage);
                    Platform.runLater(() -> showErrorPopup(errorMessage));
                    return null;
                }
            }
        } catch (Exception e) {
            String errorMessage = e.getMessage() != null ? e.getMessage() : "Failed to connect to exe/pubsubtoken endpoint";
            log.error("exe/pubsubtoken request error: {}", errorMessage);
            Platform.runLater(() -> showErrorPopup(errorMessage));
            return null;
        }
    }

    private void handleAddIbSettings(Map<String, Object> tradeData) {
        log.info("Handling add_ib_settings request: {}", tradeData);

        try {
            // Extract contract details from tradeData
            String symbol = (String) tradeData.get("Symbol");
            String localSymbol = (String) tradeData.get("LocalSymbol");
            String securityType = (String) tradeData.get("SecurityType");
            String maturityDate = (String) tradeData.get("MaturityDate");
            String currency = (String) tradeData.get("Currency");
            String exchange = (String) tradeData.get("Exchange");
            String Ibsymbol = (String) tradeData.get("Ibsymbol");
            String randomId = heartbeat_connection_id;

            // Validate required fields
            if (symbol == null || securityType == null || currency == null || exchange == null) {
                log.error("Missing required fields in add_ib_settings data: {}", tradeData);
                sendIbSettingsToApi(randomId, localSymbol, securityType, "LMT", exchange, symbol, "", "", currency,
                        "", "", maturityDate, "", "", true, "Missing required fields");
                return;
            }

            // Create contract using TwsEngine
            Contract contract = twsEngine.createContract(
                    securityType,
                    Ibsymbol,
                    exchange,
                    currency,
                    null, // strike (not needed for FUT)
                    null, // right (not needed for FUT)
                    symbol, // baseSymbol (same as symbol for FUT)
                    maturityDate,
                    Ibsymbol // tradingClass (using localSymbol as initial value)
            );

            // Fetch contract details synchronously
            List<ContractDetails> contractDetailsList = twsEngine.reqContractDetailsSync(contract);
            if (contractDetailsList == null || contractDetailsList.isEmpty()) {
                log.error("No contract details found for contract: {}", contract.toString());
                sendIbSettingsToApi(randomId, localSymbol, securityType, "LMT", exchange, symbol, "", "", currency,
                        "", "", maturityDate, "", "", true, "No contract details found");
                return;
            }

            // Extract details from the first contract
            ContractDetails contractDetails = contractDetailsList.get(0);
            Contract detailedContract = contractDetails.contract();
            String lotSize = detailedContract.multiplier() != null ? detailedContract.multiplier() : "";
            String minTick = contractDetails.minTick() > 0 ? String.valueOf(contractDetails.minTick()) : "";
            String conId = String.valueOf(detailedContract.conid());
            localSymbol = detailedContract.localSymbol() != null ? detailedContract.localSymbol() : "";
            String tradingClass = detailedContract.tradingClass() != null ? detailedContract.tradingClass() : "";
            String marketRule = contractDetails.marketRuleIds() != null ? contractDetails.marketRuleIds() : "";


            sendIbSettingsToApi(
                    randomId,
                    localSymbol,
                    securityType,
                    "LMT",
                    exchange,
                    symbol,
                    conId,
                    Ibsymbol,
                    currency,
                    lotSize,
                    minTick,
                    maturityDate,
                    tradingClass,
                    marketRule,
                    false,
                    ""
            );

        } catch (Exception e) {
            log.error("Error processing add_ib_settings: {}", e.getMessage(), e);
            String errorMsg = e.getMessage() != null ? e.getMessage() : "Failed to process add_ib_settings";
            sendIbSettingsToApi(
                    (String) tradeData.get("random_id"),
                    (String) tradeData.get("LocalSymbol"),
                    (String) tradeData.get("SecurityType"),
                    "LMT",
                    (String) tradeData.get("Exchange"),
                    (String) tradeData.get("Symbol"),
                    "",
                    "",
                    (String) tradeData.get("Currency"),
                    "",
                    "",
                    (String) tradeData.get("MaturityDate"),
                    "",
                    "",
                    true,
                    errorMsg
            );
        }
    }

    private void sendIbSettingsToApi(String randomId, String localSymbol, String instType, String orderType,
                                     String exchange, String symbol, String conId, String ibSymbol, String currency,
                                     String lotSize, String minTick, String maturityDate, String tradingClass,
                                     String marketRule, boolean error, String errorMsg) {
        log.info("Sending IB settings to API for random_id: {}", randomId);

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost("https://api.pickmytrade.io/v2/save_ib_setting_via_app");

            // Prepare payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("random_id", randomId != null ? randomId : "");
            payload.put("local_symbol", localSymbol != null ? localSymbol : "");
            payload.put("inst_type", instType != null ? instType : "");
            payload.put("order_type", orderType != null ? orderType : "LMT");
            payload.put("exchange", exchange != null ? exchange : "");
            payload.put("symbol", symbol != null ? symbol : "");
            payload.put("con_id", conId != null ? conId : "");
            payload.put("ib_symbol", ibSymbol != null ? ibSymbol : "");
            payload.put("currency", currency != null ? currency : "");
            payload.put("lot_size", lotSize != null ? lotSize : "");
            payload.put("min_tick", minTick != null ? minTick : "");
            payload.put("maturity_date", maturityDate != null ? maturityDate : "");
            payload.put("trading_class", tradingClass != null ? tradingClass : "");
            payload.put("market_rule", marketRule != null ? marketRule : "");
            payload.put("error", error);
            payload.put("error_msg", errorMsg != null ? errorMsg : "");

            String jsonPayload = gson.toJson(payload);
            log.info("IB settings payload: {}", jsonPayload);

            post.setEntity(new StringEntity(jsonPayload));
            post.setHeader("Content-Type", "application/json");

            // Add Authorization header
            Token tokenRecord = DatabaseConfig.getToken();
            String authToken = tokenRecord != null ? tokenRecord.getToken() : "";
            ConnectionEntity conn = DatabaseConfig.getConnectionEntity();
            String connName = conn != null ? conn.getConnectionName() : "";
            if (!authToken.isEmpty() && !connName.isEmpty()) {
                post.setHeader("Authorization", authToken + "_" + connName);
            }

            try (CloseableHttpResponse response = client.execute(post)) {
                String responseText = EntityUtils.toString(response.getEntity());
                log.info("IB settings API response: {}", responseText);

                if (response.getStatusLine().getStatusCode() == 200) {
                    log.info("Successfully sent IB settings for random_id: {}", randomId);
                } else {
                    log.error("Failed to send IB settings for random_id: {}. Response: {}", randomId, responseText);
                }
            }
        } catch (Exception e) {
            log.error("Error sending IB settings to API: {}", e.getMessage(), e);
        }
    }

    private void startPubSubSubscriber(String subscriptionId, String accessTokenString) {
        if (!pubsubIsStarting.compareAndSet(false, true)) {
            log.warn("Subscriber start already in progress for subscription: {}, skipping", subscriptionId);
            return;
        }

        try {
            log.info("Attempting to start server connection subscriber for subscription: {}", subscriptionId);
            String projectId = "pickmytrader"; // Replace with your actual project ID

            // Save token for monitor usage
            pubsubAccessTokenRef.set(accessTokenString);

            // Build credentials
            AccessToken accessToken = new AccessToken(accessTokenString, new Date(System.currentTimeMillis() + 3600 * 1000));
            UpdatableGoogleCredentials credentials = new UpdatableGoogleCredentials(accessToken);
            pubsubCredentialsRef.set(credentials);
            log.info("Initial Google Cloud credentials created with AccessToken");

            // Validate subscription
            if (!validateSubscription(projectId, subscriptionId, pubsubCredentialsRef.get())) {
                log.warn("Subscription validation failed, attempting token refresh...");
                String newAccessTokenString = refreshAccessToken(accessTokenString);
                if (newAccessTokenString == null) {
                    log.error("Failed to refresh token. Marking as disconnected");
                    updatePubSubStatus("disconnected");
                    pubsubLastMessageReceived.set(System.currentTimeMillis() - 61_000); // Force monitor retry
                    return;
                }
                accessToken = new AccessToken(newAccessTokenString, new Date(System.currentTimeMillis() + 3600 * 1000));
                pubsubCredentialsRef.get().updateAccessToken(accessToken);
                pubsubAccessTokenRef.set(newAccessTokenString);
                log.info("Updated Google Cloud credentials with new AccessToken after refresh");
            }

            // Define message receiver

            MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
                try {
                    String messageData = message.getData().toStringUtf8();
                    Map<String, String> attributes = message.getAttributesMap();
                    log.info("Received server connection message ID: {} with data: {} and attributes: {}",
                            message.getMessageId(), messageData, attributes);

                    pubsubLastMessageReceived.set(System.currentTimeMillis());
                    updatePubSubStatus("connected");
                    consumer.ack();
                    Map<String, Object> tradeData = gson.fromJson(messageData, new TypeToken<Map<String, Object>>() {
                    }.getType());

                    // Heartbeat
                    if (tradeData.containsKey("heartbeat")) {
                        log.debug("Received heartbeat message ID: {}", message.getMessageId());
                        pubsubLastMessageReceived.set(System.currentTimeMillis());
                        executor.submit(this::sendHeartbeatToApiOnce);
                        log.debug("Acknowledged server connection heartbeat message ID: {}", message.getMessageId());
                        return;
                    }

                    if (tradeData.containsKey("add_ib_settings")) {
                        log.debug("Received add_ib_settings message ID: {}", message.getMessageId());
                        pubsubLastMessageReceived.set(System.currentTimeMillis());
                        Map<String, Object> new_trade_settings = (Map<String, Object>) tradeData.get("add_ib_settings");
                        executor.submit(() -> {

                            handleAddIbSettings(new_trade_settings);

                        });
                        log.debug("Acknowledged server connection message ID: {} (add_ib_settings)", message.getMessageId());
                        consumer.ack();
                        return;
                    }

                    if (tradeData.containsKey("send_logs")) {
                        log.debug("Received heartbeat message ID: {}", message.getMessageId());
                        pubsubLastMessageReceived.set(System.currentTimeMillis());

                        executor.submit(this::uploadLogs);
                        log.debug("Acknowledged server connection heartbeat message ID: {}", message.getMessageId());
                        return;
                    }

                    // Token update
                    if (tradeData.containsKey("access_token")) {
                        String newAccessTokenString = (String) tradeData.get("access_token");
                        log.info("Received new access token via server connection: {}", newAccessTokenString);
                        pubsubLastMessageReceived.set(System.currentTimeMillis());

                        AccessToken newAccessToken = new AccessToken(newAccessTokenString, new Date(System.currentTimeMillis() + 3600 * 1000));
                        pubsubCredentialsRef.get().updateAccessToken(newAccessToken);
                        pubsubAccessTokenRef.set(newAccessTokenString);
                        log.info("Updated Google Cloud credentials with new AccessToken");


                        log.debug("Acknowledged server connection message ID: {} (token update)", message.getMessageId());
                        return;
                    }


                    if (tradeData.containsKey("random_alert_key")) {
                        String serverDataSent = (String) tradeData.get("server_data_sent");
                        try {
                            pubsubLastMessageReceived.set(System.currentTimeMillis());
                            String randomAlertKey = (String) tradeData.get("random_alert_key");
                            log.info("Received random alert key: {} with server_data_sent: {}", randomAlertKey, serverDataSent);
                            executor.submit(() -> sendotradeconfirmationToApiOnce(randomAlertKey));

                        }catch (Exception e) {
                            log.error("Failed to parse server_data_sent timestamp: {}", serverDataSent, e);

                        }
                    }

                    // Restart order status processor if particular message received
                    if (tradeData.containsKey("restart_order_status")) {
                        log.info("Received request to restart order status processor");
                        pubsubLastMessageReceived.set(System.currentTimeMillis());
                        restartOrderStatusProcessor();
                        log.debug("Acknowledged server connection message ID: {} (restart order status)", message.getMessageId());
                        return;
                    }

                    // Check for server_data_sent timestamp
                    if (tradeData.containsKey("server_data_sent")) {
                        String serverDataSent = (String) tradeData.get("server_data_sent");
                        try {
                            // Parse ISO 8601 string with microseconds directly
                            Instant instant = Instant.parse(serverDataSent);
                            pubsubLastMessageReceived.set(System.currentTimeMillis());
                            // Convert to java.util.Date (if you need compatibility)
                            Date sentTime = Date.from(instant);

                            long timeDiff = System.currentTimeMillis() - sentTime.getTime();
                            if (timeDiff > 60_000) { // Older than 30 seconds
                                log.info("Message ID {} is older than 30 seconds ({} ms), acknowledging without processing",
                                        message.getMessageId(), timeDiff);
                                consumer.ack();
                                log.debug("Acknowledged server connection message ID: {} (old message)", message.getMessageId());
                                return;
                            }
                        } catch (Exception e) {
                            log.error("Failed to parse server_data_sent timestamp: {}", serverDataSent, e);
                            consumer.nack();
                            return;
                        }
                    }

                    // Handle trade data
                    String user = attributes.get("user");
                    tradeData.put("user", user);

                    Platform.runLater(() -> {
                        consoleLog.appendText(String.format("Alert received: %s for user: %s\n",
                                tradeData.get("alert"), user));
                    });

                    try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost("http://localhost:8880/place-trade");
                        String payload = gson.toJson(tradeData);
                        log.debug("Sending trade request to HTTP server: {}", payload);
                        post.setEntity(new StringEntity(payload));
                        post.setHeader("Content-Type", "application/json");
                        try (CloseableHttpResponse apiResponse = client.execute(post)) {
                            String responseText = EntityUtils.toString(apiResponse.getEntity());
                            log.info("HTTP server response: {}", responseText);
                            Map<String, Object> apiResult = gson.fromJson(responseText, new TypeToken<Map<String, Object>>() {
                            }.getType());
                            if (!(boolean) apiResult.get("success")) {
                                log.error("Trade placement failed: {}", apiResult.get("message"));
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error calling HTTP trade server: {}", e.getMessage(), e);
                    }

                    consumer.ack();
                    log.debug("Acknowledged server connection message ID: {}", message.getMessageId());
                } catch (Exception e) {
                    log.error("Error processing server connection message ID {}: {}", message.getMessageId(), e.getMessage(), e);
                    consumer.nack();
                }
            };

            // Attempt to start subscriber
            int maxRetries = 5;
            int attempt = 0;
            long retryDelayMs = 5000; // 5 seconds
            ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

            while (attempt < maxRetries) {
                try {
                    log.debug("Attempting to create server connection subscriber (attempt {}/{})", attempt + 1, maxRetries);
                    Subscriber newSubscriber = Subscriber.newBuilder(subscriptionName, receiver)
                            .setCredentialsProvider(FixedCredentialsProvider.create(pubsubCredentialsRef.get()))
                            .build();
                    pubsubSubscriberRef.set(newSubscriber);
                    newSubscriber.startAsync().awaitRunning(10, TimeUnit.SECONDS);
                    log.info("✅ Subscriber started for subscription: {}", subscriptionId);
                    executor.submit(this::sendHeartbeatToApiOnce);
                    Platform.runLater(() -> consoleLog.appendText("server connection subscriber started successfully for subscription: " + subscriptionId + "\n"));
                    updatePubSubStatus("connected");
                    return; // Exit on success
                } catch (Exception e) {
                    attempt++;
                    log.error("Failed to start server connection subscriber (attempt {}/{}): {}", attempt, maxRetries, e.getMessage(), e);
                    if (e.getCause() != null) {
                        log.error("Caused by: {}", e.getCause().getMessage(), e.getCause());
                    }
                    updatePubSubStatus("retry " + attempt);
                    if (attempt >= maxRetries) {
                        log.warn("Max retries reached for server connection subscriber connection. Notifying monitor to retry...");
                        updatePubSubStatus("disconnected");
                        pubsubLastMessageReceived.set(System.currentTimeMillis() - 61_000);
                        return; // Let monitor handle retry
                    }
                    try {
                        log.debug("Waiting {} ms before retrying server connection subscriber", retryDelayMs);
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ie) {
                        log.error("Retry sleep interrupted for server connection subscriber", ie);
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        } finally {
            pubsubIsStarting.set(false); // Release the guard
        }
    }

    private void monitorPubSubConnection(String subscriptionId) {
        log.info("Starting server connection monitor for subscription: {}", subscriptionId);

        Runnable monitorTask = () -> {
            try {
                long now = System.currentTimeMillis();
                if (now - pubsubLastMessageReceived.get() > 180_000) { // 60 seconds without messages
                    log.warn("No messages in 180s → reconnecting...");
                    Subscriber currentSubscriber = pubsubSubscriberRef.getAndSet(null);
                    if (currentSubscriber != null) {
                        try {
                            currentSubscriber.stopAsync().awaitTerminated(5, TimeUnit.SECONDS);
                            log.info("Current server connection subscriber stopped for reconnection");
                        } catch (Exception e) {
                            log.warn("Failed to stop subscriber cleanly: {}", e.getMessage());
                        }
                    }
                    updatePubSubStatus("disconnected");

                    String token = pubsubAccessTokenRef.get();
                    if (token == null) {
                        log.error("No token available, will retry on next monitor cycle");
                        // Let scheduleAtFixedRate handle retry
                    } else {
                        executor.submit(() -> startPubSubSubscriber(subscriptionId, token));
                    }
                }
            } catch (Exception e) {
                log.error("Monitor error: {}", e.getMessage(), e);
                updatePubSubStatus("error");
            }
        };

        // Schedule monitor task to run every 10 seconds
        pubsubScheduler.scheduleAtFixedRate(monitorTask, 60, 60, TimeUnit.SECONDS);
    }


    private void updatePubSubStatus(String status) {
        log.debug("Updating server connection status to: {}", status);
        Platform.runLater(() -> {
            if (status.startsWith("retry")) {
                websocketLight.setFill(Color.ORANGE);
                websocketStatusLabel.setText("PickMyTrade server connection Status: \nRetrying (" + status + ")");
            } else {
                switch (status) {
                    case "connected":
                        websocketLight.setFill(Color.GREEN);
                        websocketStatusLabel.setText("PickMyTrade server connection Status: \nConnected");
                        break;
                    case "connecting":
                        websocketLight.setFill(Color.ORANGE);
                        websocketStatusLabel.setText("PickMyTrade server connection Status: \nConnecting");
                        break;
                    case "disconnected":
                    case "error":
                        websocketLight.setFill(Color.RED);
                        websocketStatusLabel.setText("PickMyTrade server connection Status: \n" + (status.equals("error") ? "Error" : "Disconnected"));
                        break;
                }
            }
        });
    }

    private void restartOrderStatusProcessor() {
        log.info("Restarting order status processor...");
        TwsEngine.orderStatusProcessingStarted.set(false);
        TwsEngine.orderStatusExecutor.shutdownNow();
        try {
            if (!TwsEngine.orderStatusExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Order status executor did not terminate gracefully");
            }
        } catch (InterruptedException e) {
            log.error("Interrupted during order status executor shutdown", e);
        }
        orderExecutor.shutdownNow();
        try {
            if (!orderExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Order executor did not terminate gracefully");
            }
        } catch (InterruptedException e) {
            log.error("Interrupted during order executor shutdown", e);
        }

        // Optionally clear the queue if you want to discard pending items
        // TwsEngine.orderStatusQueue.clear();
        TwsEngine.orderStatusExecutor = Executors.newSingleThreadExecutor();
        twsEngine.startOrderStatusProcessing();
        log.info("Order status processor restarted successfully");


        orderExecutor = Executors.newSingleThreadExecutor();
        orderExecutor.submit(this::scheduleOrderSender);
        log.info("Order send processor restarted successfully");
    }

    private void showErrorPopup(String errorMessage) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Error");
            alert.setHeaderText(null);
            alert.setContentText(errorMessage);
            alert.initModality(Modality.APPLICATION_MODAL);
            alert.showAndWait();
        });
    }

    private Map<String, Object> loginAndGetToken(Map<String, String> payload) {
        log.info("Sending login request to API with payload: {}", gson.toJson(payload));
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost("https://api.pickmytrade.io/v2/exe_Login");
            post.setEntity(new StringEntity(gson.toJson(payload)));
            post.setHeader("Content-Type", "application/json");
            log.debug("Executing HTTP POST request to login endpoint");

            try (CloseableHttpResponse response = client.execute(post)) {
                String responseText = EntityUtils.toString(response.getEntity());
                log.info("Received login response: {}", responseText);

                Map<String, Object> responseMap;
                try {
                    if (responseText.trim().startsWith("{")) {
                        responseMap = gson.fromJson(responseText, new TypeToken<Map<String, Object>>() {
                        }.getType());
                    } else {
                        log.warn("API returned a string instead of a JSON object: {}", responseText);
                        responseMap = new HashMap<>();
                        responseMap.put("success", false);
                        responseMap.put("message", responseText);
                        return responseMap;
                    }
                } catch (Exception parseEx) {
                    log.error("Failed to parse response as JSON: {}. Raw response: {}", parseEx.getMessage(), responseText);
                    responseMap = new HashMap<>();
                    responseMap.put("success", false);
                    responseMap.put("message", "Parsing error: " + parseEx.getMessage() + ". Raw response: " + responseText);
                    return responseMap;
                }

                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode == 200 && responseMap.containsKey("error") && !(boolean) responseMap.get("error")) {
                    if (responseMap.containsKey("id")) {
                        log.info("Login request successful, saving token");
                        String token = (String) responseMap.get("id");
                        DatabaseConfig.saveOrUpdateToken(token);

                        if (responseMap.containsKey("connection_name")) {
                            log.debug("Saving connection name: {}", responseMap.get("connection_name"));
                            DatabaseConfig.saveConnection((String) responseMap.get("connection_name"));
                        }
                        responseMap.put("success", true);
                        responseMap.put("message", "");
                    } else if (responseMap.containsKey("connections")) {
                        log.info("Login response contains connections, returning for selection");
                        return responseMap;
                    } else {
                        String error_string = (String) responseMap.get("data");
                        log.warn("Login response missing expected fields (id or connections): {}", error_string);
                        responseMap.put("success", false);
                        responseMap.put("message", error_string);
                    }
                } else {
                    log.warn("Login request failed: {}", responseMap.getOrDefault("data", responseMap.getOrDefault("message", "Login failed")));
                    responseMap.put("success", false);
                    responseMap.put("message", responseMap.getOrDefault("data", responseMap.getOrDefault("message", "Login failed")).toString());
                }
                return responseMap;
            }
        } catch (Exception e) {
            String errorMessage = e.getMessage() != null ? e.getMessage() : "Failed to connect to the server";
            log.error("Login request error: {}", errorMessage);
            return Map.of("success", false, "message", errorMessage);
        }
    }

    private String showConnectionPopup(List<String> connections) {
        log.info("Showing connection selection popup with {} connections", connections.size());
        Stage popup = new Stage();
        popup.initModality(Modality.APPLICATION_MODAL);
        popup.setTitle("Select Connection");
        VBox layout = new VBox(20);
        layout.setPadding(new Insets(20));
        Label label = new Label("Please select a connection:");
        label.setFont(Font.font("Arial", 14));
        ComboBox<String> dropdown = new ComboBox<>();
        dropdown.getItems().add("Select a connection");
        dropdown.getItems().addAll(connections);
        dropdown.setValue("Select a connection");
        dropdown.setStyle("-fx-font-size: 16; -fx-pref-height: 40; -fx-border-radius: 20");
        Button okButton = new Button("OK");
        okButton.setStyle("-fx-background-color: #dc143c; -fx-text-fill: white; -fx-border-radius: 20; -fx-pref-height: 40");
        okButton.setOnAction(e -> {
            log.debug("User confirmed connection selection");
            popup.close();
        });
        layout.getChildren().addAll(label, dropdown, okButton);
        layout.setAlignment(Pos.CENTER);
        Scene scene = new Scene(layout, 400, 200);
        popup.setScene(scene);
        popup.showAndWait();
        String selected = dropdown.getValue();
        log.info("Selected connection: {}", selected.equals("Select a connection") ? "None" : selected);
        return "Select a connection".equals(selected) ? null : selected;
    }

    private Scene createConnectionStatusScene(Stage stage) {
        log.info("Creating connection status scene");
        VBox layout = new VBox(20);
        layout.setPadding(new Insets(20));
        layout.setStyle("-fx-background-color: white;");

        Label title = new Label("Connection Status");
        title.setFont(Font.font("Arial", 16));
        title.setTextAlignment(TextAlignment.CENTER);

        HBox connectionsLayout = new HBox(20);
        VBox twsBox = new VBox(10);
        twsBox.setAlignment(Pos.CENTER);
        twsBox.setStyle("-fx-border-width: 1; -fx-border-color: black;");
        twsStatusLabel = new Label("TWS Connection Status: \nConnecting...");
        twsStatusLabel.setFont(Font.font("Arial", 12));
        twsLight = new Circle(10, Color.ORANGE);
        twsBox.getChildren().addAll(twsStatusLabel, twsLight);

        VBox websocketBox = new VBox(10);
        websocketBox.setAlignment(Pos.CENTER);
        websocketBox.setStyle("-fx-border-width: 1; -fx-border-color: black;");
        websocketStatusLabel = new Label("PickMyTrade server connection Status: \nConnecting...");
        websocketStatusLabel.setFont(Font.font("Arial", 12));
        websocketLight = new Circle(10, Color.ORANGE);
        websocketBox.getChildren().addAll(websocketStatusLabel, websocketLight);

        connectionsLayout.getChildren().addAll(twsBox, websocketBox);
        connectionsLayout.setAlignment(Pos.CENTER);

        consoleLog = new TextArea();
        consoleLog.setEditable(false);
        consoleLog.setFont(Font.font("Courier New", 10));

        HBox bottomLayout = new HBox(10);
        Button openPortalButton = new Button("Open pickmytrade web portal");
        openPortalButton.setStyle("-fx-background-color: blue; -fx-text-fill: white; -fx-padding: 8 15");
        openPortalButton.setOnAction(e -> {
            log.info("Opening PickMyTrade web portal");
            getHostServices().showDocument("https://app.pickmytrade.io");
        });

        Button sendLogsButton = new Button("Send Logs");
        sendLogsButton.setStyle("-fx-background-color: #6c757d; -fx-text-fill: white; -fx-padding: 8 15");
        sendLogsButton.setOnAction(e -> executor.submit(this::uploadLogs));

        Label versionLabel = new Label("App Version: " + app_version);
        versionLabel.setFont(Font.font("Arial", 10));
        versionLabel.setTextFill(Color.GRAY);
        bottomLayout.getChildren().addAll(openPortalButton, sendLogsButton, new Region(), versionLabel);
        HBox.setHgrow(bottomLayout.getChildren().get(2), Priority.ALWAYS);

        layout.getChildren().addAll(title, connectionsLayout, new Label("IB Logs:"), consoleLog, bottomLayout);
        return new Scene(layout, 900, 600);
    }

    private void continuouslyCheckTwsConnection(Stage stage) {
        log.info("Starting continuous TWS connection check");
        consoleLog.clear();
        boolean placetrade_new = false;
        twsEngine.twsConnect();

        while (true) {
            try {
                Thread.sleep(2000);
                if (twsEngine.isConnected()) {
                    log.debug("TWS is connected");
                    updateTwsStatus("connected");

                    if (!placetrade_new) {
                        log.info("Starting place order service");
                        placetrade_new = true;
                        placeOrderService.setTwsEngine(twsEngine);
                        placeRemainingTpSlOrderWrapper(appStartTime);
                    }
                    retrycheck_count = 1;
                } else {
                    log.info("TWS disconnected or not yet connected. Attempting to connect...");
                    updateTwsStatus("disconnected");
                    placetrade_new = false;
                    twsEngine.disconnect();
                    twsEngine = new TwsEngine();
                    connectToTwsWithRetries(stage);
                }

                Thread.sleep(2000);
            } catch (InterruptedException e) {
                log.info("TWS connection check interrupted");
                break;
            } catch (Exception e) {
                log.error("Error checking TWS connection: {}", e.getMessage());
                updateTwsStatus("error");
            }
        }
        log.info("TWS connection check loop stopped");
    }

    private void connectToTwsWithRetries(Stage stage) {
        int maxRetries = Integer.MAX_VALUE;
        int delaySeconds = 10;
        int attempt = 1;

        log.info("Attempting to connect to TWS with indefinite retries");
        updateTwsStatus("connecting");

        while (true) {
            try {
                log.info("Attempt {} to connect to TWS...", attempt);
                twsEngine.twsConnect();
                Thread.sleep(2000);
                if (twsEngine.isConnected()) {
                    log.info("TWS connected successfully on attempt {}", attempt);
                    updateTwsStatus("connected");
                    return;
                } else {
                    log.warn("TWS connection failed on attempt {}", attempt);
                    updateTwsStatus("retry " + attempt);
                    twsEngine.disconnect();
                    twsEngine = new TwsEngine();
                }
            } catch (Exception e) {
                log.error("Error during TWS connection attempt {}: {}", attempt, e.getMessage());
                updateTwsStatus("retry " + attempt);
            }

            attempt++;
            try {
                log.debug("Waiting {} seconds before next retry", delaySeconds);
                Thread.sleep(delaySeconds * 1000);
            } catch (InterruptedException e) {
                log.info("TWS connection retry sleep interrupted");
                updateTwsStatus("disconnected");
                return;
            }
        }
    }

    private void updateTwsStatus(String status) {
        log.debug("Updating TWS status to: {}", status);
        Platform.runLater(() -> {
            if (status.startsWith("retry")) {
                twsLight.setFill(Color.ORANGE);
                twsStatusLabel.setText("TWS Connection Status: \nRetrying (" + status + ")");
            } else {
                switch (status) {
                    case "connected":
                        twsLight.setFill(Color.GREEN);
                        twsStatusLabel.setText("TWS Connection Status: \nConnected");
                        consoleLog.clear();
                        break;
                    case "connecting":
                        twsLight.setFill(Color.ORANGE);
                        twsStatusLabel.setText("TWS Connection Status: \nConnecting");
                        break;
                    case "disconnected":
                    case "error":
                        twsLight.setFill(Color.RED);
                        twsStatusLabel.setText("TWS Connection Status: \n" + (status.equals("error") ? "Error" : "Disconnected"));
                        break;
                }
            }
        });
    }

    private void sendOrdersToApiOnce() {
        try {
            Thread.sleep(2000);
            List<OrderClient> orders = DatabaseConfig.getOrderClientsNotSentToServer();
            Token tokenRecord = DatabaseConfig.getToken();
            String authToken = tokenRecord != null ? tokenRecord.getToken() : "";
            ConnectionEntity conn = DatabaseConfig.getConnectionEntity();
            String connName = conn != null ? conn.getConnectionName() : "";

            for (OrderClient order : orders) {
                log.debug("Sending order from Main App: {}", order);
                Map<String, Object> data = placeOrderService.orderToDict(order);

                try (CloseableHttpClient client = HttpClients.createDefault()) {
                    HttpPost post = new HttpPost("https://api.pickmytrade.io/v2/exe_save_orders");
                    String payload = gson.toJson(data);
                    log.info("payload to send to API: {}", payload);
                    post.setEntity(new StringEntity(payload));
                    post.setHeader("Authorization", authToken + "_" + connName);
                    post.setHeader("Content-Type", "application/json");

                    try (CloseableHttpResponse response = client.execute(post)) {
                        String responseText = EntityUtils.toString(response.getEntity());
                        log.info("Order API response: {}", responseText);

                        Map<String, Object> updateFields = new HashMap<>();
                        if (response.getStatusLine().getStatusCode() == 200) {
                            updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Pushed.toString());
                        } else {
                            updateFields.put("sent_to_server", OrderClient.SentToServerStatus.Failed.toString());
                        }
                        DatabaseConfig.updateOrderClient(order, updateFields);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error sending orders to API: {}", e.getMessage());
            log.error("Error sending orders: {}", e.getMessage());
        }
    }


    private void sendHeartbeatToApiOnce() {

//        heartbeat_auth_token = (String) response.get("connection_name");
//        heartbeat_connection_id = (String) response.get("id");

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            String userKey = heartbeat_connection_id + "_" + heartbeat_auth_token;

            HttpPost post = new HttpPost("https://api.pickmytrade.io/wbsk/exe_heartbeat");

            Map<String, String> payloadMap = new HashMap<>();
            payloadMap.put("user_key", userKey);
            String payload = gson.toJson(payloadMap);

            log.info("Heartbeat payload to send to API: {}", payload);

            post.setEntity(new StringEntity(payload));
            post.setHeader("Content-Type", "application/json");

            try (CloseableHttpResponse response = client.execute(post)) {
                String responseText = EntityUtils.toString(response.getEntity());
                log.info("Heartbeat API response: {}", responseText);

                if (response.getStatusLine().getStatusCode() == 200) {
                    log.info("Heartbeat sent successfully for user_key={}", userKey);
                } else {
                    log.warn("Heartbeat failed with status: {}", response.getStatusLine().getStatusCode());
                }
            }
        } catch (Exception e) {
            log.error("Error sending heartbeat: {}", e.getMessage(), e);
        }
    }

    private void sendotradeconfirmationToApiOnce(String trade_key) {

//        heartbeat_auth_token = (String) response.get("connection_name");
//        heartbeat_connection_id = (String) response.get("id");

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            String userKey = heartbeat_connection_id + "_" + heartbeat_auth_token;

            HttpPost post = new HttpPost("https://api.pickmytrade.io/v2/exe_trade_ack");

            Map<String, String> payloadMap = new HashMap<>();
            payloadMap.put("orders_random_id", trade_key);
            String payload = gson.toJson(payloadMap);

            log.info("tradeconfirmation payload to send to API: {}", payload);

            post.setEntity(new StringEntity(payload));
            post.setHeader("Authorization", userKey);
            post.setHeader("Content-Type", "application/json");

            log.info("Request headers:");
            for (Header header : post.getAllHeaders()) {
                log.info("  {}: {}", header.getName(), header.getValue());
            }


            try (CloseableHttpResponse response = client.execute(post)) {
                String responseText = EntityUtils.toString(response.getEntity());
                log.info("tradeconfirmation API response: {}", responseText);

                if (response.getStatusLine().getStatusCode() == 200) {
                    log.info("tradeconfirmation sent successfully for user_key={}", userKey);
                } else {
                    log.warn("Heartbeat failed with status: {}", response.getStatusLine().getStatusCode());
                }
            }
        } catch (Exception e) {
            log.error("Error sending heartbeat: {}", e.getMessage(), e);
        }
    }

    private void scheduleOrderSender() {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                sendOrdersToApiOnce();
            } catch (Exception e) {
                log.error("Scheduled task error: {}", e.getMessage());
            }
        }, 0, 2, TimeUnit.SECONDS);
    }

    private void monitorHeartbeatAck(Stage window) {
        log.info("Starting heartbeat acknowledgment monitor");
        scheduler.scheduleAtFixedRate(() -> {
            log.info("Checking heartbeat acknowledgment status");
            try {
                long now = System.currentTimeMillis();
                if (now - lastHeartbeatAck.get() > 62_000) {
                    log.warn("No heartbeat acknowledgment received for over 62 seconds. Reconnecting...");
                    cleanupAndRestartWebsocket(window);
                }
            } catch (Exception e) {
                log.warn("Error in heartbeat acknowledgment monitor: {}", e.getMessage());
                log.error("Error in heartbeat acknowledgment monitor: {}", e.getMessage());
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private void handleWebSocketMessageAsync(String message, String token, String connectionName) {
        executor.submit(() -> {
            log.info("Asynchronously processing WebSocket message: {}", message);
            try {
                Map<String, Object> response = gson.fromJson(message, new TypeToken<Map<String, Object>>() {
                }.getType());
                if ("Heartbeat acknowledged".equals(response.get("message"))) {
                    log.debug("WebSocket Heartbeat acknowledgment received");
                    lastHeartbeatAck.set(System.currentTimeMillis());
                    // updateWebsocketStatus("connected"); // Disabled as WebSocket is not used
                    return;
                }

                Map<String, Object> data = (Map<String, Object>) response.get("data");
                if (data == null) {
                    log.warn("No data field in WebSocket message: {}", message);
                    return;
                }

                if (data.containsKey("place_order")) {
                    Map<String, Object> placeOrderData = (Map<String, Object>) data.get("place_order");
                    log.info("Received trade placement request from WebSocket: {}", placeOrderData);
                    String order_Random_Id = (String) placeOrderData.get("random_alert_key");
                    log.info("Order Random ID: {}", order_Random_Id);
                    websocket.send(gson.toJson(Map.of("token", token, "trade_data_ack", order_Random_Id, "connection_name", connectionName)));
                    log.info("Processing trade placement request from WebSocket");
                    try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost("http://localhost:8880/place-trade");
                        String payload = gson.toJson(placeOrderData);
                        log.debug("Sending trade request to HTTP server: {}", payload);
                        post.setEntity(new StringEntity(payload));
                        post.setHeader("Content-Type", "application/json");
                        try (CloseableHttpResponse apiResponse = client.execute(post)) {
                            String responseText = EntityUtils.toString(apiResponse.getEntity());
                            log.info("HTTP server response: {}", responseText);
                            Map<String, Object> apiResult = gson.fromJson(responseText, new TypeToken<Map<String, Object>>() {
                            }.getType());
                            if (!(boolean) apiResult.get("success")) {
                                log.error("Trade placement failed: {}", apiResult.get("message"));
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error calling HTTP trade server: {}", e.getMessage(), e);
                    }
                } else if (data.containsKey("send_logs")) {
                    log.info("Uploading logs as requested by server");
                    uploadLogs();
                } else if (data.containsKey("ib_rollover")) {
                    log.info("Triggering IB rollover");
                    twsEngine.getIbRollover((List<Integer>) data.get("ib_rollover"));
                }
            } catch (Exception e) {
                log.error("Error processing WebSocket message asynchronously: {}", e.getMessage(), e);
            }
        });
    }

    private void checkWebsocket(Stage window) {
        log.info("Starting WebSocket check");
        String token;
        String connectionName;

        try {
            while (true) {
                log.debug("Retrieving token from database");
                Token tokenRecord = DatabaseConfig.getToken();
                if (tokenRecord != null) {
                    token = tokenRecord.getToken();
                    log.info("Token retrieved: {}", token);
                    break;
                }
                log.debug("No token found, retrying in 1 second");
                Thread.sleep(1000);
            }
            log.debug("Retrieving connection entity from database");
            ConnectionEntity connection = DatabaseConfig.getConnectionEntity();
            connectionName = connection != null ? connection.getConnectionName() : null;
            if (connectionName == null) {
                log.warn("No connection name found, setting stop flag");
                return;
            }
        } catch (Exception e) {
            log.error("Error retrieving token or connection: {}", e.getMessage());
            // updateWebsocketStatus("error"); // Disabled as WebSocket is not used
            return;
        }

        synchronized (websocketTasks) {
            websocketTasks.forEach(task -> task.cancel(true));
            websocketTasks.clear();
        }

        log.info("Checking connection entity: {}", connectionName);
        lastHeartbeatAck.set(System.currentTimeMillis());

        String websocketId = UUID.randomUUID().toString();
        log.info("Creating new WebSocket connection with ID: {}", websocketId);
        final String wsId = websocketId;
        URI uri = URI.create("wss://api.pickmytrade.io/wbsk/live");
        log.info("Initializing WebSocket connection to: {}", uri);

        websocket = new WebSocketClient(uri) {
            @Override
            public void onOpen(ServerHandshake handshakedata) {
                log.info("Connected to WebSocket server");
                // updateWebsocketStatus("connected"); // Disabled as WebSocket is not used

                synchronized (websocketTasks) {
                    websocketTasks.add(executor.submit(() -> sendHeartbeat(token, connectionName)));
                    websocketTasks.add(executor.submit(() -> sendAccountDataToServer()));
                }
            }

            @Override
            public void onMessage(String message) {
                log.info("Received WebSocket message, delegating to async handler: {}", message);
                handleWebSocketMessageAsync(message, token, connectionName);
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.info("WebSocket connection closed: {} - {}", code, reason);
                // updateWebsocketStatus("error"); // Disabled as WebSocket is not used
                lastHeartbeatAck.set(System.currentTimeMillis() - 65_000);
                log.info("setting lastHeartbeatAck to 65 seconds ago");
            }

            @Override
            public void onError(Exception ex) {
                log.error("WebSocket error occurred: {}", ex.getMessage(), ex);
                // updateWebsocketStatus("error"); // Disabled as WebSocket is not used
            }

            @Override
            public void onWebsocketPong(WebSocket conn, Framedata f) {
                log.info("Received Pong from server: {}", new String(f.getPayloadData().array()));
            }

            private void sendHeartbeat(String token, String connectionName) {
                log.info("Starting WebSocket heartbeat");
                while (isOpen() && !Thread.currentThread().isInterrupted()) {
                    try {
                        send(gson.toJson(Map.of("token", token, "heartbeat", "ping", "connection_name", connectionName)));
                        log.debug("WebSocket [{}] token {} heartbeat: ping", wsId, token);
                        Thread.sleep(20_000);
                    } catch (InterruptedException e) {
                        log.info("Heartbeat task interrupted");
                    } catch (Exception e) {
                        log.error("Heartbeat error: {}", e.getMessage());
                    }
                }
                log.info("Heartbeat task stopped");
            }
        };

        connectWebsocket();
    }

    private void connectWebsocket() {
        try {
            log.debug("Attempting to connect to WebSocket");
            // updateWebsocketStatus("connecting"); // Disabled as WebSocket is not used
            websocket.setConnectionLostTimeout(0);
            websocket.connectBlocking();
            log.info("WebSocket connection connection established successfully");
            lastHeartbeatAck.set(System.currentTimeMillis());
        } catch (InterruptedException e) {
            log.info("WebSocket connection interrupted");
            // updateWebsocketStatus("error"); // Disabled as WebSocket is not used
        } catch (Exception e) {
            log.error("Initial WebSocket connection failed: {}", e.getMessage());
            // updateWebsocketStatus("error"); // Disabled as WebSocket is not used
        }
    }

    private void cleanupAndRestartWebsocket(Stage window) {
        if (websocketCleanupLock.tryLock()) {
            try {
                lastHeartbeatAck.set(System.currentTimeMillis());
                log.info("Acquired lock for WebSocket cleanup and restart");
                synchronized (websocketTasks) {
                    websocketTasks.forEach(task -> task.cancel(true));
                    websocketTasks.clear();
                }
                if (websocket != null) {
                    try {
                        websocket.closeBlocking();
                        log.info("WebSocket closed successfully");
                    } catch (Exception e) {
                        log.error("Error closing WebSocket: {}", e.getMessage());
                    } finally {
                        websocket = null;
                    }
                }
                websocketExecutor.submit(() -> checkWebsocket(window));
                log.info("WebSocket cleanup and restart completed");
            } finally {
                websocketCleanupLock.unlock();
                log.debug("Released lock for WebSocket cleanup and restart");
            }
        } else {
            log.info("WebSocket cleanup and restart already in progress, skipping");
        }
    }

    private List<AccountData> retrieveAccountData() {
        log.debug("Retrieving account data from database");
        try {
            return DatabaseConfig.getAccountData();
        } catch (SQLException e) {
            log.error("Error retrieving account data: {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    private void sendAccountDataToServer() {

        while (!Thread.currentThread().isInterrupted()) {
            String token;
            String connectionName;



            try {
                while (true) {
                    log.debug("Retrieving token from database");
                    Token tokenRecord = DatabaseConfig.getToken();
                    if (tokenRecord != null) {
                        token = tokenRecord.getToken();
                        log.info("Token retrieved: {}", token);
                        break;
                    }
                    log.debug("No token found, retrying in 1 second");
                    Thread.sleep(1000);
                }
                log.debug("Retrieving connection entity from database");
                ConnectionEntity connection = DatabaseConfig.getConnectionEntity();
                connectionName = connection != null ? connection.getConnectionName() : null;
                if (connectionName == null) {
                    log.warn("No connection name found, setting stop flag");
                    return;
                }
            } catch (Exception e) {
                log.error("Error retrieving token or connection: {}", e.getMessage());
                // updateWebsocketStatus("error"); // Disabled as WebSocket is not used
                return;
            }
            log.info("Starting task to send account data to server with token: {} and connectionName: {}", token, connectionName);
            try {
                Thread.sleep(15_000);
                log.debug("Retrieving account data for sending");
                List<AccountData> accountData = retrieveAccountData();
                log.info("Retrieved {} account data entries", accountData.size());

                if (!accountData.isEmpty()) {
                    List<String> accountIds = accountData.stream()
                            .map(AccountData::getAccountId)
                            .distinct()
                            .collect(Collectors.toList());
                    log.debug("Extracted account IDs: {}", accountIds);

                    try (CloseableHttpClient client = HttpClients.createDefault()) {
                        HttpPost post = new HttpPost("https://api.pickmytrade.io/v2/exe_save_accounts");
                        String payload = gson.toJson(Map.of("accounts", accountIds));
                        post.setEntity(new StringEntity(payload));
                        post.setHeader("Authorization", token + "_" + connectionName);
                        post.setHeader("Content-Type", "application/json");

                        log.info("Request headers:");
                        for (Header header : post.getAllHeaders()) {
                            log.info("  {}: {}", header.getName(), header.getValue());
                        }

                        log.info("Request URL: {}", post.getURI());
                        log.info("Request method: {}", post.getMethod());
                        log.info("Sending account data to server with payload: {}", payload);

                        try (CloseableHttpResponse response = client.execute(post)) {
                            Thread.sleep(1000);
                            log.info("Response headers:");
                            for (Header header : response.getAllHeaders()) {
                                log.info("  {}: {}", header.getName(), header.getValue());
                            }

                            String responseText = EntityUtils.toString(response.getEntity());
                            int statusCode = response.getStatusLine().getStatusCode();
                            log.info("Account data server response - Status: {}, Body: {}", statusCode, responseText);

                            if (statusCode == 200) {
                                log.info("Account data sent to server successfully");
                                break;
                            } else {
                                log.warn("Failed to send account data - Status: {}, Response: {}", statusCode, responseText);
                            }
                        }
                    }
                } else {
                    log.info("No account data to send");
                }
            } catch (InterruptedException e) {
                log.info("Account data send task interrupted");
                break;
            } catch (Exception e) {
                log.error("Error sending account data to server", e);
            }
        }
        log.info("Account data send task stopped");
    }

    private void placeRemainingTpSlOrderWrapper(long time_var) {
        log.info("Starting task to place remaining TP/SL orders");
        try {
            placeOrderService.placeRemainingTpSlOrder(time_var);
        } catch (Exception e) {
            log.error("Error in placeRemainingTpSlOrder: {}", e.getMessage());
        }
        log.info("Place remaining TP/SL orders task stopped");
    }

    private void resetconnection(){
        DatabaseConfig.emptyconnectionsTable();
    }

    private void uploadLogs() {
        String token = heartbeat_connection_id;
        log.info("Uploading logs with token: {}", token);
        try {
            File logsDir = new File(System.getProperty("log.path", "./logs"));
            if (!logsDir.exists() || !logsDir.isDirectory()) {
                log.warn("Logs directory does not exist: {}", logsDir.getAbsolutePath());
                return;
            }

            // Collect all files starting with "log"
            File[] logFiles = logsDir.listFiles((dir, name) -> name.toLowerCase().startsWith("log"));
            if (logFiles == null || logFiles.length == 0) {
                log.warn("No log files found in directory: {}", logsDir.getAbsolutePath());
                return;
            }

            File zipperFile = new File(logsDir, "logs.zip");
            log.debug("Zipping {} log files into {}", logFiles.length, zipperFile.getAbsolutePath());

            try (FileOutputStream fos = new FileOutputStream(zipperFile);
                 ZipOutputStream zos = new ZipOutputStream(fos)) {

                for (File logFile : logFiles) {
                    log.debug("Adding log file: {}", logFile.getName());
                    zos.putNextEntry(new ZipEntry(logFile.getName()));
                    Files.copy(logFile.toPath(), zos);
                    zos.closeEntry();
                }
            }

            try (CloseableHttpClient client = HttpClients.createDefault()) {
                HttpPost post = new HttpPost("https://api.pickmytrade.io/v2/upload_log");
                MultipartEntityBuilder builder = MultipartEntityBuilder.create();
                builder.addBinaryBody("file", zipperFile);
                post.setEntity(builder.build());
                post.setHeader("Authorization", token);

                log.debug("Uploading zipped log files...");
                try (CloseableHttpResponse response = client.execute(post)) {
                    String responseText = EntityUtils.toString(response.getEntity());
                    log.info("Log upload response: {}", responseText);
                    if (response.getStatusLine().getStatusCode() == 200) {
                        log.info("All log files uploaded successfully!");
                    } else {
                        log.error("Failed to upload log files: {}", responseText);
                    }
                }
            }

            log.debug("Deleting temporary zip file {}", zipperFile.getAbsolutePath());
            Files.deleteIfExists(zipperFile.toPath());
        } catch (Exception e) {
            log.error("Error uploading logs: {}", e.getMessage(), e);
        }
    }

}