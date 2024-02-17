/*
 * Copyright(c) 2012 Saarland University - Information Systems Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package querycompilation;

import dbs_project.storage.Type;

/**
 * Simple helper structure to describe columns.
 *
 * @author Stefan Richter
 */
public class ColumnDescriptor {

    /**
     *
     * @param columnName
     * @param type
     * @param columnData
     */
    public ColumnDescriptor(String columnName, Type type, Object columnData) {
        this.columnData = columnData;
        this.columnName = columnName;
        this.type = type;
    }
    private final Object columnData;
    private final String columnName;
    private final Type type;

    /**
     *
     * @return
     */
    public Type getType() {
        return type;
    }

    /**
     *
     * @return
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     *
     * @return the actual column data structure
     */
    public Object getColumnData() {
        return columnData;
    }
}
