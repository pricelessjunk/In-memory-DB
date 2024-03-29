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

package dbs_project.exceptions;

/**
 * Thrown if the table does not contain a requested column.
 */
public class NoSuchColumnException extends Exception {

    /**
     *
     */
    public NoSuchColumnException() {
        super();
    }

    /**
     * @param message
     * @param cause
     */
    public NoSuchColumnException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public NoSuchColumnException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public NoSuchColumnException(Throwable cause) {
        super(cause);
    }
}
