package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {
    private final Jedis jedis;

    public RedisRepository() {
        this.jedis = new Jedis("localhost");
    }

    public List<String> getLastTenSearches() {
        // TODO implement last 10 searchs
        return jedis.lrange("lastTenSearches", 0, -1);
    }

    public void addSearch(String search){
        while(jedis.llen("lastTenSearches") > 10){
            jedis.rpop("lastTenSearches");
        }
        jedis.lpush("lastTenSearches", search);
    }
}
