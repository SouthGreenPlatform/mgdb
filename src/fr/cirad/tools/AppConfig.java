/**
 * *****************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 <CIRAD>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU Affero
 * General Public License V3.
 ******************************************************************************
 */
package fr.cirad.tools;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

/**
 * The Class AppConfig.
 */
@Configuration
@PropertySource("classpath:config.properties")
public class AppConfig {

    /**
     * The env.
     */
    @Autowired
    private Environment env;

    /**
     * Db server cleanup.
     *
     * @return the string
     */
    public String dbServerCleanup() {
        return env.getProperty("dbServerCleanup");
    }

    public int sessionTimeout() {
        return Integer.parseInt(env.getProperty("sessionTimeout"));
    }

    public String get(String sPropertyName) {
        return env.getProperty(sPropertyName);
    }
}
