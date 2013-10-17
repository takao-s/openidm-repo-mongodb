/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright © 2011 ForgeRock AS.
 * Portions Copyrighted 2013 Takao Sekiguchi.
 * All rights reserved.
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 *
 * You can obtain a copy of the License at
 * http://forgerock.org/license/CDDLv1.0.html
 * See the License for the specific language governing
 * permission and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * Header Notice in each file and include the License file
 * at http://forgerock.org/license/CDDLv1.0.html
 * If applicable, add the following below the CDDL Header,
 * with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */
package org.forgerock.openidm.repo.mongodb.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class JsonReader {
    private BufferedReader br;

    public String getJsonConfig() {
        String path = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String file = "repo.mongodb.json";
        StringBuffer sb = buildBuffer(path, file);
        return sb.toString();
    }

    public String getJsonUsers() {
        String path = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String file = "users.json";
        StringBuffer sb = buildBuffer(path, file);
        return sb.toString();
    }

    private StringBuffer buildBuffer(String path, String file) {
        StringBuffer sb = new StringBuffer();
        try {
            FileReader in = new FileReader(path + file);
            br = new BufferedReader(in);
            String line;
            while( (line = br.readLine()) != null ) {
                sb.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb;
    }
    
    public static void main(String[] args) {
        JsonReader jr = new JsonReader();
        String s = jr.getJsonConfig();
        System.out.println(s);
    }
    
}
