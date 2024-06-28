/*
package org.apache.seatunnel.transform.python;

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.exception.TransformException;

public class PythonEngineFactory {

    public static PythonEngine getPythonEngine(PythonEngineType engineType) {
        switch (engineType) {
            case PYTHON_ZETA:
            case PYTHON_INTERNAL:
                return new PythonEngine();
        }
        throw new TransformException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported python engine type: %s", engineType));
    }

    public enum PythonEngineType {
        PYTHON_ZETA,
        PYTHON_INTERNAL
    }
}*/
