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

import static java.util.Objects.requireNonNull;
import static org.knime.cloud.aws.redshift.connector2.utility.load.RedshiftDBLoaderNodeSettings.SETTINGS_KEY_TARGET_FOLDER;

import org.knime.base.filehandling.remote.dialog.DialogComponentRemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.database.node.component.dbrowser.DBTableSelectorDialogComponent;
import org.knime.database.node.component.dbrowser.SettingsModelDBMetadata;
import org.knime.database.node.io.load.DBLoaderNode.DialogDelegate;

/**
 * Node dialog components and corresponding settings for {@link RedshiftDBLoaderNodeComponents}.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class RedshiftDBLoaderNodeComponents {

    private final DialogDelegate m_dialogDelegate;

    private final DBTableSelectorDialogComponent m_tableNameComponent;

    private final SettingsModelDBMetadata m_tableNameModel;

    private final DialogComponentRemoteFileChooser m_targetFolderComponent;

    private final SettingsModelString m_targetFolderModel;

    /**
     * @param dialogDelegate
     */
    public RedshiftDBLoaderNodeComponents(final DialogDelegate dialogDelegate) {
        m_dialogDelegate = requireNonNull(dialogDelegate, "dialogDelegate");
        m_tableNameModel = createTableNameModel();
        m_tableNameComponent = createTableNameComponent(m_tableNameModel, dialogDelegate);
        m_targetFolderModel = createTargetFolderModel();
        m_targetFolderComponent = createTargetFolderComponent(m_targetFolderModel);
    }

    /**
     * Gets the delegate of the node dialog the components have been created for.
     *
     * @return a {@link DialogDelegate} object.
     */
    public DialogDelegate getDialogDelegate() {
        return m_dialogDelegate;
    }

    /**
     * Gets the database table name dialog component.
     *
     * @return a {@link DBTableSelectorDialogComponent} object.
     */
    public DBTableSelectorDialogComponent getTableNameComponent() {
        return m_tableNameComponent;
    }

    /**
     * Gets the database table name settings model.
     *
     * @return a {@link SettingsModelDBMetadata} object.
     */
    public SettingsModelDBMetadata getTableNameModel() {
        return m_tableNameModel;
    }

    /**
     * Creates the table name component.
     *
     * @param tableNameModel the already created table name settings model.
     * @param delegate {@link DialogDelegate}
     * @return a {@link DBTableSelectorDialogComponent} object.
     */
    protected DBTableSelectorDialogComponent createTableNameComponent(final SettingsModelDBMetadata tableNameModel,
        final DialogDelegate delegate) {
        return new DBTableSelectorDialogComponent(m_tableNameModel, 1, false, null, "Select a table",
            "Database Metadata Browser", true, (keys, type) -> {
            return delegate.getDialog().createFlowVariableModel(keys, type);
        });
    }

    /**
     * Creates the table name settings model.
     *
     * @return a {@link SettingsModelDBMetadata} object.
     */
    protected SettingsModelDBMetadata createTableNameModel() {
        return new SettingsModelDBMetadata("tableName");
    }

    /**
     * Gets the target folder chooser dialog component.
     *
     * @return a {@link RemoteFileChooserPanel} object.
     */
    public DialogComponentRemoteFileChooser getTargetFolderComponent() {
        return m_targetFolderComponent;
    }

    /**
     * Gets the settings model of the target folder for copying/uploading the file.
     *
     * @return a {@linkplain SettingsModelString} object that contains a folder path string.
     */
    public SettingsModelString getTargetFolderModel() {
        return m_targetFolderModel;
    }

    /**
     * Creates the target folder component.
     *
     * @param targetFolderModel the already created target folder settings model.
     * @return a {@link DialogComponentRemoteFileChooser} object.
     */
    protected DialogComponentRemoteFileChooser
        createTargetFolderComponent(final SettingsModelString targetFolderModel) {
        return new DialogComponentRemoteFileChooser(targetFolderModel, "Target folder", false, 2, "targetHistory",
            RemoteFileChooser.SELECT_DIR, getDialogDelegate().getDialog()
                .createFlowVariableModel(SETTINGS_KEY_TARGET_FOLDER, FlowVariable.Type.STRING));
    }

    /**
     * Creates the target folder settings model.
     *
     * @return a {@link SettingsModelString} object.
     */
    protected SettingsModelString createTargetFolderModel() {
        return new SettingsModelString(SETTINGS_KEY_TARGET_FOLDER, "");
    }
}
