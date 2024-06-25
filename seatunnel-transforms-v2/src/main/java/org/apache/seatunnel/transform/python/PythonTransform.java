package org.apache.seatunnel.transform.python;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;

/**
 * @description:
 * @author: wangjing
 * @createDate: 2024/6/20
 */
@Slf4j
public class PythonTransform extends AbstractCatalogSupportTransform {

    public static final String PLUGIN_NAME = "PythonScript";

    public static final Option<String> PYTHON_SCRIPT_FILE_ID =
            Options.key("python_script_file_id").stringType().noDefaultValue().withDescription("The Python script file id");

    private transient PythonEngine pythonEngine;

    private final String pythonScriptFileId;

    public PythonTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        pythonScriptFileId = config.get(PYTHON_SCRIPT_FILE_ID);
    }

    @Override
    public void open() {
        pythonEngine = new PythonEngine(pythonScriptFileId);
        pythonEngine.init(); // 初始化 Python 引擎
    }

    @Override
    public void close() {
        pythonEngine.close();
    }

    private void tryOpen() {
        if (pythonEngine == null) {
            open();
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }


    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        tryOpen();
        return pythonEngine.transformByPython(inputRow);
    }


    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

}

