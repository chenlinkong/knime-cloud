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
 *   Apr 8, 2019 (jfalgout): created
 */
package org.knime.cloud.aws.comprehend.sentiment.node;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.cloud.aws.comprehend.BaseComprehendOperation;
import org.knime.cloud.aws.comprehend.ComprehendUtils;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.ext.textprocessing.data.Document;
import org.knime.ext.textprocessing.data.DocumentBuilder;
import org.knime.ext.textprocessing.data.DocumentCell;
import org.knime.ext.textprocessing.data.DocumentValue;
import org.knime.ext.textprocessing.data.SectionAnnotation;

import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.model.DetectSentimentRequest;
import com.amazonaws.services.comprehend.model.DetectSentimentResult;
import com.amazonaws.services.comprehend.model.SentimentScore;

/**
 *
 * @author jfalgout
 */
class SentimentOperation extends BaseComprehendOperation {
    // Language of the source text to be analyzed
    private final String m_sourceLanguage;

    SentimentOperation(final ConnectionInformation cxnInfo, final String textColumnName, final String sourceLanguage, final DataTableSpec outputSpec) {
        super(cxnInfo, textColumnName, outputSpec);
        this.m_sourceLanguage = sourceLanguage;
    }

    @Override
    public void compute(final RowInput in, final RowOutput out, final ExecutionContext exec, final long rowCount) throws CanceledExecutionException, InterruptedException {

        // Create a connection to the Comprehend service in the provided region
        AmazonComprehend comprehendClient = ComprehendUtils.getClient(m_cxnInfo);

        // Access the input data table
        int textColumnIdx = in.getDataTableSpec().findColumnIndex(m_textColumnName);
        long inputRowIndex = 0;
        long rowCounter = 0;

        // For each input row, grab the text column, make the call to Comprehend
        // and push each of the syntax elements to the output.
        DataRow inputRow = null;
        while ((inputRow = in.poll()) != null) {
            // Check for cancel and update the row progress
            ++rowCounter;
            exec.checkCanceled();
            if (rowCount > 0) {
                exec.setProgress(rowCounter / (double) rowCount, "Processing row " + rowCounter + " of " + rowCount);
            }

            // Grab the text to evaluate
            Document inputDoc = ((DocumentValue) inputRow.getCell(textColumnIdx)).getDocument();
            String textValue = inputDoc.getDocumentBodyText();

            DetectSentimentRequest detectSentimentRequest =
                new DetectSentimentRequest()
                    .withText(textValue)
                    .withLanguageCode(ComprehendUtils.LANG_MAP.getOrDefault(m_sourceLanguage, "en"));

            DetectSentimentResult detectSentimentResult = comprehendClient.detectSentiment(detectSentimentRequest);

            // Create cells containing the output data.
            // Copy the input field values to the output.
            int numInputColumns = inputRow.getNumCells();
            DataCell[] cells = new DataCell[numInputColumns + 6];
            for (int i = 0; i < numInputColumns; i++) {
                cells[i] = inputRow.getCell(i);
            }

            // Grab scores for each sentiment category.
            SentimentScore score = detectSentimentResult.getSentimentScore();

            // Build the output document. Copy the input and add metadata about the sentiment and copy the text.
            DocumentBuilder builder = new DocumentBuilder(inputDoc);
            builder.addMetaInformation("Sentiment", detectSentimentResult.getSentiment());
            builder.addMetaInformation("Sentiment_mixed_score", Float.toString(score.getMixed()));
            builder.addMetaInformation("Sentiment_neutral_score", Float.toString(score.getNeutral()));
            builder.addMetaInformation("Sentiment_negative_score", Float.toString(score.getNegative()));
            builder.addMetaInformation("Sentiment_positive_score", Float.toString(score.getPositive()));
            builder.addSection(textValue, SectionAnnotation.UNKNOWN);
            Document outputDoc = builder.createDocument();

            // Copy the results to the new columns in the output.
            cells[numInputColumns] = new DocumentCell(outputDoc);
            cells[numInputColumns + 1] = new StringCell(detectSentimentResult.getSentiment());
            cells[numInputColumns + 2] = new DoubleCell(score.getMixed());
            cells[numInputColumns + 3] = new DoubleCell(score.getPositive());
            cells[numInputColumns + 4] = new DoubleCell(score.getNeutral());
            cells[numInputColumns + 5] = new DoubleCell(score.getNegative());

            // Make row key unique with the input row number and the sequence number of each token
            RowKey key = new RowKey("Row " + inputRowIndex);

            // Create a new data row and push it to the output container.
            DataRow row = new DefaultRow(key, cells);
            out.push(row);

            ++inputRowIndex;
        }

        return;
    }

}
