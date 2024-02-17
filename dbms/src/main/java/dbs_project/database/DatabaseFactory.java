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
package dbs_project.database;

import dbs_project.databaseImpl.DatabaseImpl;
import dbs_project.util.Factory;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * Singleton factory that creates new Database instances. Change
 * createInstance() to return a new instance of your own implementation. Hence,
 * you are allowed to modify this class.
 */
public enum DatabaseFactory implements Factory<Database, Void> {

    INSTANCE;

    /**
     * Delegates to createInstance() and is actually not called for testing.
     *
     * @param param parameter of type P for object construction
     * @return new instance of your database implementation.
     */
    @Override
    public Database createInstance(Void param) {
        return createInstance();
    }

    /**
     *
     * Change this methos to return a new instance of your own Database
     * implementation.
     *
     * @return new instance of your database implementation.
     */
    @Override
    public Database createInstance() {
        Database db = new DatabaseImpl();

        try {
            db.startUp();
        } catch (IOException ex) {
            Logger.getLogger(DatabaseFactory.class.getName()).log(Level.SEVERE, null, ex);
        }

        return db;
    }

}
