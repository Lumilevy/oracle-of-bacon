package com.serli.oracle.of.bacon.api;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import com.serli.oracle.of.bacon.repository.MongoDbRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository.GraphItem;
import com.serli.oracle.of.bacon.repository.RedisRepository;

import net.codestory.http.annotations.Get;
import net.codestory.http.convert.TypeConvert;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;

public class APIEndPoint {
    private final Neo4JRepository neo4JRepository;
    private final ElasticSearchRepository elasticSearchRepository;
    private final RedisRepository redisRepository;
    private final MongoDbRepository mongoDbRepository;

    public APIEndPoint() {
        neo4JRepository = new Neo4JRepository();
        elasticSearchRepository = new ElasticSearchRepository();
        redisRepository = new RedisRepository();
        mongoDbRepository = new MongoDbRepository();
    }

    @Get("bacon-to?actor=:actorName")
    public String getConnectionsToKevinBacon(String actorName) {
        redisRepository.addSearch(actorName);
        List<GraphItem> result = neo4JRepository.getConnectionsToKevinBacon(actorName);
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = "";
        List<Map> listMap = new ArrayList<Map>();
        for (GraphItem graph : result) {
            Map map = new HashMap<>();
            map.put("data", graph);
            listMap.add(map);
        }

        jsonString += TypeConvert.toJson(listMap);
        return jsonString;
    }

    @Get("suggest?q=:searchQuery")
    public List<String> getActorSuggestion(String searchQuery) throws IOException {
        return Arrays.asList("Bannos, Steve", "Senanayake, Niro", "Niro, Juan Carlos", "de la Rua, Niro",
                "Niro, Simão");
    }

    @Get("last-searches")
    public List<String> last10Searches() {
        return redisRepository.getLastTenSearches();
    }

    @Get("actor?name=:actorName")
    public String getActorByName(String actorName) {
        return mongoDbRepository.getActorByName(actorName).map(e -> e.toJson()).orElse("");
    }
}
