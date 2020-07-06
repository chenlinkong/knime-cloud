/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 */
package org.knime.cloud.aws.redshift.connector2.utility.load;

import static java.nio.file.Files.delete;
import static java.util.Arrays.asList;
import static org.knime.base.filehandling.NodeUtils.encodePath;
import static org.knime.base.filehandling.remote.files.RemoteFileFactory.createRemoteFile;
import static org.knime.core.util.FileUtil.createTempFile;
import static org.knime.database.agent.metadata.DBMetaDataHelper.createDBTableSpec;
import static org.knime.database.util.RemoteFiles.copyLocalFile;
import static org.knime.database.util.RemoteFiles.resolveFileName;
import static org.knime.datatype.mapping.DataTypeMappingDirection.KNIME_TO_EXTERNAL;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JPanel;

import org.apache.commons.lang3.StringUtils;
import org.apache.orc.CompressionKind;
import org.apache.orc.TypeDescription;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.bigdata.fileformats.node.writer.AbstractFileFormatWriter;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCTypeMappingService;
import org.knime.bigdata.fileformats.orc.writer.OrcKNIMEWriter;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetTypeMappingService;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.util.Pair;
import org.knime.database.DBTableSpec;
import org.knime.database.agent.loader.DBLoadTableParameters;
import org.knime.database.agent.loader.DBLoader;
import org.knime.database.agent.metadata.DBMetadataReader;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.model.DBColumn;
import org.knime.database.model.DBTable;
import org.knime.database.node.component.dbrowser.SettingsModelDBMetadata;
import org.knime.database.node.io.load.impl.ConnectedLoaderNode;
import org.knime.database.port.DBDataPortObject;
import org.knime.database.port.DBDataPortObjectSpec;
import org.knime.database.port.DBSessionPortObject;
import org.knime.database.port.DBSessionPortObjectSpec;
import org.knime.database.session.DBSession;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;

/**
 * Implementation of the loader node for the Redshift database.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class RedshiftDBLoaderNode
    extends ConnectedLoaderNode<RedshiftDBLoaderNodeComponents, RedshiftDBLoaderNodeSettings> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(RedshiftDBLoaderNode.class);

    private static Box createBox(final boolean horizontal) {
        final Box box;
        if (horizontal) {
            box = new Box(BoxLayout.X_AXIS);
            box.add(Box.createVerticalGlue());
        } else {
            box = new Box(BoxLayout.Y_AXIS);
            box.add(Box.createHorizontalGlue());
        }
        return box;
    }

    private static JPanel createPanel() {
        final JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        return panel;
    }

    @Override
    protected void buildDialog(final DialogBuilder builder, final List<DialogComponent> dialogComponents,
        final RedshiftDBLoaderNodeComponents customComponents) {
        final JPanel optionsPanel = createPanel();
        final Box optionsBox = createBox(false);
        optionsPanel.add(optionsBox);
        optionsBox.add(customComponents.getTableNameComponent().getComponentPanel());
        optionsBox.add(customComponents.getTargetFolderComponent().getComponentPanel());
        builder.addTab(Integer.MAX_VALUE, "Options", optionsPanel, true);
//        final JPanel advancedPanel = createPanel();
//        final Box advancedBox = createBox(false);
//        advancedPanel.add(advancedBox);
//        advancedBox.add(customComponents.getLoaderComponent().getComponentPanel());
//        builder.addTab(Integer.MAX_VALUE, "Advanced", advancedPanel, true);
    }

    @Override
    protected RedshiftDBLoaderNodeComponents createCustomDialogComponents(final DialogDelegate dialogDelegate) {
        return new RedshiftDBLoaderNodeComponents(dialogDelegate);
    }

    @Override
    protected RedshiftDBLoaderNodeSettings createCustomModelSettings(final ModelDelegate modelDelegate) {
        return new RedshiftDBLoaderNodeSettings(modelDelegate);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<DialogComponent> createDialogComponents(final RedshiftDBLoaderNodeComponents customComponents) {
        return  asList(customComponents.getTableNameComponent(), customComponents.getTargetFolderComponent());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<SettingsModel> createSettingsModels(final RedshiftDBLoaderNodeSettings customSettings) {
        return asList(customSettings.getTableNameModel(), customSettings.getTargetFolderModel());
    }

    @Override
    protected DBDataPortObjectSpec configureModel(final PortObjectSpec[] inSpecs,
        final List<SettingsModel> settingsModels, final RedshiftDBLoaderNodeSettings customSettings)
        throws InvalidSettingsException {
        final DBSessionPortObjectSpec sessionPortObjectSpec = (DBSessionPortObjectSpec)inSpecs[1];
        if (inSpecs[2] instanceof ConnectionInformationPortObjectSpec) {
            final ConnectionInformationPortObjectSpec con = (ConnectionInformationPortObjectSpec)inSpecs[2];
            final String protocol = con.getConnectionInformation().getProtocol();
            if (!protocol.startsWith("s3")) {
                throw new InvalidSettingsException("Please use an Amazon S3 connection");
            }
        }

        if (StringUtils.isBlank(customSettings.getTargetFolderModel().getStringValue())) {
            throw new InvalidSettingsException("The target folder is missing.");
        }
        final SettingsModelDBMetadata tableNameModel = customSettings.getTableNameModel();
        //Redshift column names are case insensitive and always folded to lower case
        //https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
        validateColumns(true, createModelConfigurationExecutionMonitor(sessionPortObjectSpec.getDBSession()),
            (DataTableSpec)inSpecs[0], sessionPortObjectSpec, tableNameModel.toDBTable());
        return super.configureModel(inSpecs, settingsModels, customSettings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DBDataPortObject load(final ExecutionParameters<RedshiftDBLoaderNodeSettings> parameters) throws Exception {
        final DataTableSpec tableSpec = parameters.getRowInput().getDataTableSpec();
        final DBSessionPortObject sessionPortObject = parameters.getSessionPortObject();
        final DBSession session = sessionPortObject.getDBSession();
        final ExecutionContext exec = parameters.getExecutionContext();
        final RedshiftDBLoaderNodeSettings redshiftSettings = parameters.getCustomSettings();
        final DBTable dbTable = redshiftSettings.getTableNameModel().toDBTable();
        final DataTypeMappingConfiguration<SQLType> typeMappingConfiguration =
                sessionPortObject.getKnimeToExternalTypeMapping().resolve(DBTypeMappingRegistry.getInstance()
                    .getDBTypeMappingService(session.getDBType()), KNIME_TO_EXTERNAL);
        final ConsumptionPath[] consumptionPaths = typeMappingConfiguration.getConsumptionPathsFor(tableSpec);
        final DBColumn[] dbColumns = createDBTableSpec(tableSpec, consumptionPaths);
        final DBTableSpec dbTableSpe = getDBTableSpecification(exec, dbTable, session);
        //Redshift column names are case insensitive and always folded to lower case
        //https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
        exec.setMessage("Validating input columns");
        exec.checkCanceled();
        validateColumns(true, dbColumns, dbTableSpe);

        final ConnectionMonitor<?> connectionMonitor = new ConnectionMonitor<>();

        //Write local temporary file
        final Path temporaryFile = createTempFile("knime2db", ".orc").toPath();
//        final FileWriterSettings fileWriterSettings = new FileWriterSettings();
//        fileWriterSettings.setCharacterEncoding("UTF8");
//        fileWriterSettings.setColSeparator("|");
//        writeCsv(parameters.getRowInput(), temporaryFile, fileWriterSettings, exec);
        try (AutoCloseable temporaryFileDeleter = () -> delete(temporaryFile);
                AutoCloseable connectionMonitorCloser = () -> connectionMonitor.closeAll()) {

            writeTemporaryFile(parameters.getRowInput(), temporaryFile, dbColumns, exec);

            final CloudRemoteFile<?> targetFile =
                (CloudRemoteFile<?>)getTargetFile(parameters, redshiftSettings, temporaryFile, connectionMonitor);

            try (AutoCloseable targetFileDeleter = new RemoteFileDeleter(redshiftSettings, targetFile)) {
                uploadTemporaryFile(exec, temporaryFile, targetFile);
                // Load the data
                final Pair<RedshiftDBLoaderNodeSettings, RemoteFile<?>> pair = new Pair<>(redshiftSettings, targetFile);
                session.getAgent(DBLoader.class).load(exec, new DBLoadTableParameters<>(null, dbTable, pair));
                // Output
                return new DBDataPortObject(sessionPortObject,
                    session.getAgent(DBMetadataReader.class).getDBDataObject(exec, dbTable.getSchemaName(),
                        dbTable.getName(), sessionPortObject.getExternalToKnimeTypeMapping().resolve(
                            DBTypeMappingRegistry.getInstance().getDBTypeMappingService(session.getDBType()),
                            DataTypeMappingDirection.EXTERNAL_TO_KNIME)));
            }
        }


    }

    /**
     * Deleter class for try-with-resources
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    private static class RemoteFileDeleter implements AutoCloseable {

        private final RedshiftDBLoaderNodeSettings m_customSettings;

        private final RemoteFile<?> m_targetFile;

        RemoteFileDeleter(final RedshiftDBLoaderNodeSettings customSettings, final RemoteFile<?> targetFile) {
            m_customSettings = customSettings;
            m_targetFile = targetFile;
        }

        @Override
        public void close() throws Exception {
            if (m_targetFile.exists() && !m_targetFile.delete()) {
                m_customSettings.getModelDelegate().setWarning("The target file could not be deleted.");
            }
        }
    }

    private static void uploadTemporaryFile(final ExecutionContext exec, final Path temporaryFile,
        final RemoteFile<?> targetFile) throws Exception {
        LOGGER.debugWithFormat("Target file URI: \"%s\"", targetFile.getURI());
        copyLocalFile(temporaryFile, targetFile, exec);
        exec.setProgress(0.5, "File uploaded.");
        LOGGER.debug("The content has been copied to the target file.");
    }

    private static RemoteFile<?> getTargetFile(final ExecutionParameters<RedshiftDBLoaderNodeSettings> parameters,
        final RedshiftDBLoaderNodeSettings customSettings, final Path temporaryFile,
        final ConnectionMonitor<?> connectionMonitor) throws Exception {
        String targetFolder = customSettings.getTargetFolderModel().getStringValue();
        if (!targetFolder.endsWith("/")) {
            targetFolder += '/';
        }
        final ConnectionInformation connectionInformation =
            parameters.getConnectionInformationPortObject().getConnectionInformation();
        final RemoteFile<?> targetDirectory =
            createRemoteFile(new URI(connectionInformation.toURI() + encodePath(targetFolder)), connectionInformation,
                connectionMonitor);
        LOGGER.debugWithFormat("Target folder URI: \"%s\"", targetDirectory.getURI());
        return resolveFileName(targetDirectory, temporaryFile);
    }

    private static void writeTemporaryFile(final RowInput rowInput, final Path temporaryFile,
        final DBColumn[] dbColumns, final ExecutionContext exec) throws Exception {
        exec.checkCanceled();
        exec.setMessage("Writing temporary file...");
        final RemoteFile<Connection> file = RemoteFileFactory.createRemoteFile(temporaryFile.toUri(), null, null);
        final DataTableSpec spec = rowInput.getDataTableSpec();

        try (AbstractFileFormatWriter writer = createWriter(file, spec, dbColumns)) {
            DataRow row;
            while ((row = rowInput.poll()) != null) {
                writer.writeRow(row);
            }
        } finally {
            rowInput.close();
            LOGGER.debug("Written file " + temporaryFile + " ");
        }
        exec.setProgress(0.25, "Temporary file written.");
        exec.checkCanceled();
    }

    /**
     * Creates a {@link AbstractFileFormatWriter}
     *
     * @param file the file to write to
     * @param spec the DataTableSpec of the input
     * @param dbColumns List of columns in order of input table, renamed to generic names
     * @return AbstractFileFormatWriter
     * @throws IOException if writer cannot be initialized
     */
    private static AbstractFileFormatWriter createWriter(final RemoteFile<Connection> file, final DataTableSpec spec,
        final DBColumn[] dbColumns) throws IOException {
//        return new ParquetKNIMEWriter(file, spec, "UNCOMPRESSED", -1,
//            getParquetTypesMapping(spec, dbColumns));
        return new OrcKNIMEWriter(file, spec, -1, CompressionKind.NONE.toString(), getORCTypesMapping(spec, dbColumns));
    }

    private static DataTypeMappingConfiguration<ParquetType> getParquetTypesMapping(final DataTableSpec spec,
        final DBColumn[] inputColumns) {

        final List<ParquetType> parquetTypes = mapDBToParquetTypes(inputColumns);

        final DataTypeMappingConfiguration<ParquetType> configuration = ParquetTypeMappingService.getInstance()
            .createMappingConfiguration(DataTypeMappingDirection.KNIME_TO_EXTERNAL);

        for (int i = 0; i < spec.getNumColumns(); i++) {
            final DataColumnSpec knimeCol = spec.getColumnSpec(i);
            final DataType dataType = knimeCol.getType();
            final ParquetType parquetType = parquetTypes.get(i);
            final Collection<ConsumptionPath> consumPaths =
                ParquetTypeMappingService.getInstance().getConsumptionPathsFor(dataType);

            final Optional<ConsumptionPath> path = consumPaths.stream()
                .filter(p -> p.getConsumerFactory().getDestinationType().equals(parquetType)).findFirst();
            if (path.isPresent()) {
                configuration.addRule(dataType, path.get());
            } else {
                final String error =
                    String.format("Could not find ConsumptionPath for %s to JDBC Type %s via Parquet Type %s", dataType,
                        inputColumns[i].getColumnTypeName(), parquetType);
                LOGGER.error(error);
                throw new RuntimeException(error);
            }
        }

        return configuration;
    }

    private static List<ParquetType> mapDBToParquetTypes(final DBColumn[] inputColumns) {
        final List<ParquetType> parquetTypes = new ArrayList<>();
        for (final DBColumn dbCol : inputColumns) {
            final ParquetType parquetType = DBtoParquetTypeUtil.dbToParquetType(dbCol.getColumnType());
            if (parquetType == null) {
                throw new RuntimeException(String.format("Cannot find Parquet type for Database type %s",
                    dbCol.getColumnTypeName()));
            }
            parquetTypes.add(parquetType);
        }
        return parquetTypes;
    }

    private static DataTypeMappingConfiguration<TypeDescription> getORCTypesMapping(final DataTableSpec spec,
        final DBColumn[] inputColumns) {

        final List<TypeDescription> orcTypes = mapDBToORCTypes(inputColumns);

        final DataTypeMappingConfiguration<TypeDescription> configuration = ORCTypeMappingService.getInstance()
            .createMappingConfiguration(DataTypeMappingDirection.KNIME_TO_EXTERNAL);

        for (int i = 0; i < spec.getNumColumns(); i++) {
            final DataColumnSpec knimeCol = spec.getColumnSpec(i);
            final DataType dataType = knimeCol.getType();
            final TypeDescription parquetType = orcTypes.get(i);
            final Collection<ConsumptionPath> consumPaths =
                    ORCTypeMappingService.getInstance().getConsumptionPathsFor(dataType);

            final Optional<ConsumptionPath> path = consumPaths.stream()
                .filter(p -> p.getConsumerFactory().getDestinationType().equals(parquetType)).findFirst();
            if (path.isPresent()) {
                configuration.addRule(dataType, path.get());
            } else {
                final String error =
                    String.format("Could not find ConsumptionPath for %s to JDBC Type %s via ORC Type %s", dataType,
                        inputColumns[i].getColumnTypeName(), parquetType);
                LOGGER.error(error);
                throw new RuntimeException(error);
            }
        }

        return configuration;
    }

    private static List<TypeDescription> mapDBToORCTypes(final DBColumn[] inputColumns) {
        final List<TypeDescription> orcTypes = new ArrayList<>();
        for (final DBColumn dbCol : inputColumns) {
            final TypeDescription parquetType = DBtoORCTypeUtil.dbToParquetType(dbCol.getColumnType());
            if (parquetType == null) {
                throw new RuntimeException(String.format("Cannot find Parquet type for Database type %s",
                    dbCol.getColumnTypeName()));
            }
            orcTypes.add(parquetType);
        }
        return orcTypes;
    }
}
