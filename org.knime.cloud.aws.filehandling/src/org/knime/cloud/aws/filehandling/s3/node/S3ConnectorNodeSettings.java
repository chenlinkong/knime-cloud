/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   02.07.2020 (Alexander Bondaletov): created
 */
package org.knime.cloud.aws.filehandling.s3.node;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Base64;
import java.util.EnumSet;
import java.util.function.Consumer;

import org.knime.cloud.aws.filehandling.s3.fs.S3FileSystem;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.ReadPathAccessor;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.SettingsModelReaderFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;
import org.knime.filehandling.core.defaultnodesettings.status.NodeModelStatusConsumer;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage.MessageType;

import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

/**
 * Settings for {@link S3ConnectorNodeModel}.
 *
 * @author Alexander Bondaletov
 */
public class S3ConnectorNodeSettings {

    private static final boolean DEFAULT_NORMALIZE = true;

    private static final String DEFAULT_WORKING_DIR = S3FileSystem.PATH_SEPARATOR;

    private static final boolean DEFAULT_SSE_ENABLED = false;
    private static final String DEFAULT_SSE_MODE = SSEMode.getDefault().getKey();
    private static final String DEFAULT_KMS_KEY_ID = "";
    private static final CustomerKeySource DEFAULT_CUSTOMER_KEY_SOURCE = CustomerKeySource.SETTINGS;
    private static final String DEFAULT_CUSTOMER_KEY = "";
    private static final String DEFAULT_CUSTOMER_KEY_VAR = "";

    private static final String KEY_SOCKET_TIMEOUTS = "readWriteTimeoutInSeconds";

    private static final String KEY_NORMALIZE_PATHS = "normalizePaths";

    private static final String KEY_WORKING_DIRECTORY = "workingDirectory";

    private static final String KEY_SSE_ENABLED = "sseEnabled";
    private static final String KEY_SSE_MODE = "sseMode";
    private static final String KEY_SSE_KMS_KEY_ID = "sseKmsKeyId";
    private static final String KEY_SSE_CUSTOMER_KEY_SOURCE = "sseCustomerKeySource";
    private static final String KEY_SSE_CUSTOMER_KEY = "sseCustomerKey";
    private static final String KEY_SSE_CUSTOMER_KEY_VAR = "sseCustomerKeyVar";
    private static final String KEY_SSE_CUSTOMER_KEY_FILE = "sseCustomerKeyFile";

    private final SettingsModelIntegerBounded m_socketTimeout;

    private final SettingsModelBoolean m_normalizePath;

    private final SettingsModelString m_workingDirectory;

    private final SettingsModelBoolean m_sseEnabled;
    private final SettingsModelString m_sseMode;
    private final SettingsModelString m_kmsKeyId;

    private CustomerKeySource m_customerKeySource;
    private final SettingsModelString m_customerKey;
    private final SettingsModelString m_customerKeyVar;
    private final SettingsModelReaderFileChooser m_customerKeyFile;

    private final PortsConfiguration m_portConfig;

    private CredentialsProvider m_credentialProvider;

    /**
     * Constructor intended to be used outside of the {@link S3ConnectorNodeModel} (e.g. integration tests).
     */
    public S3ConnectorNodeSettings() {
        this(null);
    }

    /**
     * Creates new instance
     *
     * @param portsConfig Ports configuration.
     */
    public S3ConnectorNodeSettings(final PortsConfiguration portsConfig) {
        m_portConfig = portsConfig;

        m_socketTimeout = new SettingsModelIntegerBounded(KEY_SOCKET_TIMEOUTS, Math.max(1, getDefaultSocketTimeout()),
            0, Integer.MAX_VALUE);
        m_normalizePath = new SettingsModelBoolean(KEY_NORMALIZE_PATHS, DEFAULT_NORMALIZE);
        m_workingDirectory = new SettingsModelString(KEY_WORKING_DIRECTORY, DEFAULT_WORKING_DIR);

        m_sseEnabled = new SettingsModelBoolean(KEY_SSE_ENABLED, DEFAULT_SSE_ENABLED);
        m_sseMode = new SettingsModelString(KEY_SSE_MODE, DEFAULT_SSE_MODE);
        m_kmsKeyId = new SettingsModelString(KEY_SSE_KMS_KEY_ID, DEFAULT_KMS_KEY_ID);

        m_customerKeySource = DEFAULT_CUSTOMER_KEY_SOURCE;
        m_customerKey = new SettingsModelString(KEY_SSE_CUSTOMER_KEY, DEFAULT_CUSTOMER_KEY);
        m_customerKeyVar = new SettingsModelString(KEY_SSE_CUSTOMER_KEY_VAR, DEFAULT_CUSTOMER_KEY_VAR);

        if (portsConfig != null) {
            m_customerKeyFile = new SettingsModelReaderFileChooser(KEY_SSE_CUSTOMER_KEY_FILE, portsConfig,
                S3ConnectorNodeFactory.FILE_SYSTEM_CONNECTION_PORT_NAME, FilterMode.FILE);
        } else {
            m_customerKeyFile = null;
        }
    }

    private static int getDefaultSocketTimeout() {
        Duration duration =
            SdkHttpConfigurationOption.GLOBAL_HTTP_DEFAULTS.get(SdkHttpConfigurationOption.READ_TIMEOUT);
        if (duration != null) {
            return (int)duration.getSeconds();
        }
        return 0;
    }

    /**
     * @return the socketTimeout model
     */
    public SettingsModelIntegerBounded getSocketTimeoutModel() {
        return m_socketTimeout;
    }

    /**
     * @return the socketTimeout
     */
    public int getSocketTimeout() {
        return m_socketTimeout.getIntValue();
    }

    /**
     * @return the normalizePath model
     */
    public SettingsModelBoolean getNormalizePathModel() {
        return m_normalizePath;
    }

    /**
     * @return the normalizePath
     */
    public boolean getNormalizePath() {
        return m_normalizePath.getBooleanValue();
    }

    /**
     * @return the workingDirectory model
     */
    public SettingsModelString getWorkingDirectoryModel() {
        return m_workingDirectory;
    }

    /**
     * @return selected working directory or the root directory if it is not set
     */
    public String getWorkingDirectory() {
        return m_workingDirectory.getStringValue();
    }

    /**
     * @return the sseEnabled model
     */
    public SettingsModelBoolean getSseEnabledModel() {
        return m_sseEnabled;
    }

    /**
     * @return whether Sever Side Encryption is enabled.
     */
    public boolean isSseEnabled() {
        return m_sseEnabled.getBooleanValue();
    }

    /**
     * @return the sseMode model
     */
    public SettingsModelString getSseModeModel() {
        return m_sseMode;
    }

    /**
     * @return the sseMode
     */
    public SSEMode getSseMode() {
        return SSEMode.fromKey(m_sseMode.getStringValue());
    }

    /**
     * @return the kmsKeyId model
     */
    public SettingsModelString getKmsKeyIdModel() {
        return m_kmsKeyId;
    }

    /**
     * @return the kmsKeyId
     */
    public String getKmsKeyId() {
        return m_kmsKeyId.getStringValue();
    }

    /**
     * @return the customerKeySource
     */
    public CustomerKeySource getCustomerKeySource() {
        return m_customerKeySource;
    }

    /**
     * @param customerKeySource the customerKeySource to set
     */
    public void setCustomerKeySource(final CustomerKeySource customerKeySource) {
        m_customerKeySource = customerKeySource;
    }

    /**
     * @return the customerKey model
     */
    public SettingsModelString getCustomerKeyModel() {
        return m_customerKey;
    }

    /**
     * @return the customerKeyVar model
     */
    public SettingsModelString getCustomerKeyVarModel() {
        return m_customerKeyVar;
    }

    /**
     * @return the customerKeyFile model. Could be <code>null</code> in case settings instance was created without
     *         providing {@link PortsConfiguration} object.
     */
    public SettingsModelReaderFileChooser getCustomerKeyFileModel() {
        return m_customerKeyFile;
    }

    /**
     * @param credentialProvider the credentialProvider to set
     */
    public void setCredentialProvider(final CredentialsProvider credentialProvider) {
        m_credentialProvider = credentialProvider;
    }

    /**
     * Retrieves customer SSE encryption key from the appropriate location according to settings.
     *
     * @return The customer SSE encryption key or <code>null</code> if SSE-C encryption mode is not enabled.
     * @throws IOException
     * @throws InvalidSettingsException
     */
    public String getCustomerKey() throws IOException, InvalidSettingsException {
        if (!isSseEnabled() || getSseMode() != SSEMode.CUSTOMER_PROVIDED) {
            return null;
        }

        String key = null;
        switch (m_customerKeySource) {
            case SETTINGS:
                key = m_customerKey.getStringValue();
                break;
            case CREDENTIAL_VAR:
                key = getCustomerKeyFromCredentials();
                break;
            case FILE:
                key = getCustomerKeyFromFile();
                break;
        }
        return key;
    }

    private String getCustomerKeyFromCredentials() throws InvalidSettingsException {
        if (m_credentialProvider == null) {
            throw new InvalidSettingsException("No credential provider is available");
        }

        String name = m_customerKeyVar.getStringValue();
        ICredentials creds = m_credentialProvider.get(name);

        if (creds == null) {
            throw new InvalidSettingsException("Credentials not found: " + name);
        }
        return creds.getPassword();
    }

    private String getCustomerKeyFromFile() throws IOException, InvalidSettingsException {
        final NodeModelStatusConsumer statusConsumer =
            new NodeModelStatusConsumer(EnumSet.of(MessageType.ERROR, MessageType.WARNING));

        try (ReadPathAccessor accessor = m_customerKeyFile.createReadPathAccessor()) {
            FSPath path = accessor.getRootPath(statusConsumer);
            byte[] bytes = Files.readAllBytes(path);
            return Base64.getEncoder().encodeToString(bytes);
        }
    }

    /**
     * Saves the settings in this instance to the given {@link NodeSettingsWO}
     *
     * @param settings Node settings.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_socketTimeout.saveSettingsTo(settings);
        m_normalizePath.saveSettingsTo(settings);
        m_workingDirectory.saveSettingsTo(settings);
        m_sseEnabled.saveSettingsTo(settings);
        m_sseMode.saveSettingsTo(settings);
        m_kmsKeyId.saveSettingsTo(settings);
        settings.addString(KEY_SSE_CUSTOMER_KEY_SOURCE, m_customerKeySource.getKey());
        m_customerKey.saveSettingsTo(settings);
        m_customerKeyVar.saveSettingsTo(settings);
        if (m_customerKeyFile != null) {
            m_customerKeyFile.saveSettingsTo(settings);
        }
    }

    /**
     * Validates the settings in a given {@link NodeSettingsRO}
     *
     * @param settings Node settings.
     * @throws InvalidSettingsException
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_socketTimeout.validateSettings(settings);
        if (settings.containsKey(KEY_NORMALIZE_PATHS)) {
            m_normalizePath.validateSettings(settings);
        }
        if (settings.containsKey(KEY_WORKING_DIRECTORY)) {
            m_workingDirectory.validateSettings(settings);
        }
        if (settings.containsKey(KEY_SSE_ENABLED)) {
            m_sseEnabled.validateSettings(settings);
            m_sseMode.validateSettings(settings);
            m_kmsKeyId.validateSettings(settings);
            m_customerKey.validateSettings(settings);
            m_customerKeyVar.validateSettings(settings);

            if (m_customerKeyFile != null) {
                m_customerKeyFile.validateSettings(settings);
            }

            String key = settings.getString(KEY_SSE_CUSTOMER_KEY_SOURCE);
            if (CustomerKeySource.fromKey(key) == null) {
                throw new InvalidSettingsException("Invalid customer key source: " + key);
            }
        }
    }

    /**
     * Loads settings from the given {@link NodeSettingsRO}
     *
     * @param settings Node settings.
     * @throws InvalidSettingsException
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_socketTimeout.loadSettingsFrom(settings);
        if (settings.containsKey(KEY_NORMALIZE_PATHS)) {
            m_normalizePath.loadSettingsFrom(settings);
        } else {
            m_normalizePath.setBooleanValue(DEFAULT_NORMALIZE);
        }
        if (settings.containsKey(KEY_WORKING_DIRECTORY)) {
            m_workingDirectory.loadSettingsFrom(settings);
        } else {
            m_workingDirectory.setStringValue(DEFAULT_WORKING_DIR);
        }
        if (settings.containsKey(KEY_SSE_ENABLED)) {
            m_sseEnabled.loadSettingsFrom(settings);
            m_sseMode.loadSettingsFrom(settings);
            m_kmsKeyId.loadSettingsFrom(settings);
            m_customerKey.loadSettingsFrom(settings);
            m_customerKeyVar.loadSettingsFrom(settings);

            if (m_customerKeyFile != null) {
                m_customerKeyFile.loadSettingsFrom(settings);
            }
            m_customerKeySource = CustomerKeySource.fromKey(settings.getString(KEY_SSE_CUSTOMER_KEY_SOURCE));
        } else {
            m_sseEnabled.setBooleanValue(DEFAULT_SSE_ENABLED);
            m_sseMode.setStringValue(DEFAULT_SSE_MODE);
            m_kmsKeyId.setStringValue(DEFAULT_KMS_KEY_ID);
            m_customerKey.setStringValue(DEFAULT_CUSTOMER_KEY);
            m_customerKeyVar.setStringValue(DEFAULT_CUSTOMER_KEY_VAR);
            m_customerKeySource = DEFAULT_CUSTOMER_KEY_SOURCE;
        }
    }

    @Override
    protected S3ConnectorNodeSettings clone() {
        NodeSettings transferSettings = new NodeSettings("ignored");
        saveSettingsTo(transferSettings);

        S3ConnectorNodeSettings clone = new S3ConnectorNodeSettings(m_portConfig);
        try {
            clone.loadSettingsFrom(transferSettings);
        } catch (InvalidSettingsException ex) {
            throw new IllegalStateException(ex);
        }
        return clone;
    }

    /**
     * Configures {@link SettingsModelReaderFileChooser} settings.
     *
     * @param inSpecs Input specs.
     * @param msgConsumer Message consumer.
     * @throws InvalidSettingsException
     */
    public void configureFileChoosersInModel(final PortObjectSpec[] inSpecs, final Consumer<StatusMessage> msgConsumer)
        throws InvalidSettingsException {
        CheckUtils.checkState(m_customerKeyFile != null, "Settings instance was created without PortsConfiguration");
        m_customerKeyFile.configureInModel(inSpecs, msgConsumer);
    }

    /**
     * Enum representing different available S3 server-side encryption modes.
     *
     */
    public enum SSEMode {
            /**
             * SSE-S3 mode
             */
            S3("Use Amazon S3-Managed Encryption Keys (SSE-S3)", "SSE-S3", ServerSideEncryption.AES256),
            /**
             * SSE-KMS mode
             */
            KMS("Use CMKs Stored in AWS Key Management Service (SSE-KMS)", "SSE-KMS", ServerSideEncryption.AWS_KMS),
            /**
             * SSE-C mode
             */
            CUSTOMER_PROVIDED("Use customer-provided encryption keys (SSE-C)", "SSE-C", null);

        private String m_title;
        private String m_key;

        private ServerSideEncryption m_encryption;

        private SSEMode(final String title, final String key, final ServerSideEncryption encryption) {
            m_title = title;
            m_key = key;
            m_encryption = encryption;
        }

        /**
         * @return the key
         */
        public String getKey() {
            return m_key;
        }

        /**
         * @return the encryption
         */
        public ServerSideEncryption getEncryption() {
            return m_encryption;
        }

        @Override
        public String toString() {
            return m_title;
        }

        /**
         * @return The default mode.
         */
        public static SSEMode getDefault() {
            return S3;
        }

        /**
         * @param key The mode key.
         * @return The mode with the given key or the default mode in case no mode with a given key is found.
         */
        public static SSEMode fromKey(final String key) {
            for (SSEMode mode : values()) {
                if(mode.getKey().equals(key)) {
                    return mode;
                }
            }
            return getDefault();
        }
    }

    /**
     * Enum representing different sources for the customer provided key.
     */
    public enum CustomerKeySource {
            /**
             * Key in stored in node settings.
             */
            SETTINGS("Enter key", "settings"),
            /**
             * Key is stored in the credential flow variable.
             */
            CREDENTIAL_VAR("Credential flow variable", "credential"),
            /**
             * Key is stored in file.
             */
            FILE("Key file", "file");

        private String m_title;
        private String m_key;

        private CustomerKeySource(final String title, final String key) {
            m_title = title;
            m_key = key;
        }

        /**
         * @return the key
         */
        public String getKey() {
            return m_key;
        }

        /**
         * @return the title
         */
        public String getTitle() {
            return m_title;
        }

        /**
         * @return The default mode.
         */
        public static CustomerKeySource getDefault() {
            return SETTINGS;
        }

        /**
         * @param key The customer key source node settings key.
         * @return The key source with the given settings key or <code>null</code>.
         */
        public static CustomerKeySource fromKey(final String key) {
            for (CustomerKeySource source : values()) {
                if (source.getKey().equals(key)) {
                    return source;
                }
            }
            return null;
        }
    }
}
