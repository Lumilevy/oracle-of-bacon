package com.serli.oracle.of.bacon.loader.elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);

    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = ElasticSearchRepository.createClient();
        //args = new String[1];
        //args[0] = "../../neo4j-community-3.3.3/import/actors.csv";
        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        BulkProcessor bulkProcessor = BulkProcessor.builder(client::bulkAsync, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("Before bulk");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("After bulk : OK");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("After bulk : FAILURE");
            }
        }).setBulkActions(10000).setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();

        String inputFilePath = args[0];
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader.lines().forEach(line -> {
                bulkProcessor.add(new IndexRequest(line));
                System.out.println(line);
                count.incrementAndGet();
            });
        }
        bulkProcessor.flush();
        bulkProcessor.close();

        System.out.println("Inserted total of " + count.get() + " actors");

        client.close();
    }
}
