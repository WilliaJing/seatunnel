package org.apache.seatunnel.transform.python;

import lombok.NonNull;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;
import org.apache.seatunnel.transform.copy.CopyTransformConfig;
import org.apache.seatunnel.transform.replace.ReplaceTransformConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @description:
 * @author: wangjing
 * @createDate: 2024/6/20
 */
public class PythonTransform extends AbstractCatalogSupportTransform {

    public static final String PLUGIN_NAME = "Python";

    public static final Option<String> KEY_SCRIPT =
            Options.key("script").stringType().noDefaultValue().withDescription("The Python script path");

    private final String scriptPath;
    private transient PythonEngine pythonEngine;

    public PythonTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.scriptPath = config.get(KEY_SCRIPT);
        // 可以添加其他配置项的初始化逻辑
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void open() {
        pythonEngine = new PythonEngine(scriptPath);
        pythonEngine.init(); // 初始化 Python 引擎
    }

    private void tryOpen() {
        if (pythonEngine == null) {
            open();
        }
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        tryOpen();
        return pythonEngine.transformByPython(inputRow);
    }


    @Override
    protected TableSchema transformTableSchema() {
        tryOpen();
        return inputCatalogTable.getTableSchema();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    @Override
    public void close() {
        if (pythonEngine != null) {
            pythonEngine.close();
        }
    }

}

