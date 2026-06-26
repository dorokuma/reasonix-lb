package main

import (
    "os"
    "time"
    "gopkg.in/yaml.v3"
)

type AccountConfig struct {
    Name    string `yaml:"name"`
    Key     string `yaml:"key"`
    BaseURL string `yaml:"base_url"`
}

type Config struct {
    Listen        string          `yaml:"listen"`
    ProbeInterval time.Duration   `yaml:"probe_interval"`
    Accounts      []AccountConfig `yaml:"accounts"`
}

func LoadConfig(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }
    cfg := &Config{}
    if err := yaml.Unmarshal(data, cfg); err != nil {
        return nil, err
    }
    if cfg.Listen == "" {
        cfg.Listen = ":18790"
    }
    if cfg.ProbeInterval == 0 {
        cfg.ProbeInterval = 10 * time.Minute
    }
    return cfg, nil
}
