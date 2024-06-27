package org.apache.seatunnel.transform.python;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.python.PythonEngineFactory.PythonEngineType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * @description:
 * @author: wangjing
 * @createDate: 2024/6/20
 */
@Slf4j
public class PythonTransform extends MultipleFieldOutputTransform {

    public static final String PLUGIN_NAME = "PythonScript";

    public static final Option<String> PYTHON_SCRIPT_FILE_ID =
            Options.key("python_script_file_id").stringType().noDefaultValue().withDescription("The Python script file id");

    private final PythonEngineType pythonEngineType;
    private transient PythonEngine pythonEngine;

    private final String pythonScriptFileId;


    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void open() {
        super.open();
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
/*        Object[] fieldValues = new Object[fieldNames.size()];

        String jsonOutput = null;

        System.arraycopy(
                inputRow.getFields(),
                0,
                outputFieldValues,
                0,
                inputFieldLength);

        for (int i = 0; i < fieldOriginalIndexes.size(); i++) {
            fieldValues[i] =
                    clone(
                            fieldNames.get(i),
                            fieldTypes.get(i),
                            inputRow.getField(fieldOriginalIndexes.get(i)));
        }
        return fieldValues;*/
        return null;
    }

    @Override
    protected Column[] getOutputColumns() {
      /*  if (inputCatalogTable == null) {
            Column[] columns = new Column[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                columns[i] =
                        PhysicalColumn.of(fieldNames.get(i), fieldTypes.get(i), 200, true, "", "");
            }
            return columns;
        }
*/
        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();
        return columns.toArray(new Column[0]);
    }
}

