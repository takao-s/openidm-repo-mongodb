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
