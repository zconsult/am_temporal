package com.alfie.temporal.bt;

import org.apache.commons.cli.CommandLine;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

public class zConfMerger {
    private final Map<String, Object> conf;
    public zConfMerger (CommandLine cmdl) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        InputStream is = zConfMerger.class.getResourceAsStream("/defaultConfig.yaml");
        conf = yaml.load(is);
        String userConfigFile = cmdl.getOptionValue("configFile", "");
        if (!userConfigFile.isEmpty()) {
            conf.putAll(getUserConfig(userConfigFile));
        }
    }

    private static Map<String, Object> getUserConfig(String userConfig) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        InputStream is = new FileInputStream(userConfig);
        return yaml.load(is);
    }

    public Map<String, Object> getConf() {
        return conf;
    }
}
