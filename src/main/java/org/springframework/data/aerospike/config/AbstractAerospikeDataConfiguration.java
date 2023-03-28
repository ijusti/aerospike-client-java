/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.config;

import com.aerospike.client.IAerospikeClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.AerospikeExceptionTranslator;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.index.AerospikeIndexResolver;
import org.springframework.data.aerospike.index.AerospikePersistenceEntityIndexCreator;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.FilterExpressionsBuilder;
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.StatementBuilder;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.query.cache.IndexRefresher;
import org.springframework.data.aerospike.query.cache.IndexesCacheUpdater;
import org.springframework.data.aerospike.query.cache.InternalIndexOperations;

@Configuration(proxyBeanMethods = false)
public abstract class AbstractAerospikeDataConfiguration extends AerospikeDataConfigurationSupport {

    @Bean(name = "aerospikeTemplate")
    public AerospikeTemplate aerospikeTemplate(IAerospikeClient aerospikeClient,
                                               MappingAerospikeConverter mappingAerospikeConverter,
                                               AerospikeMappingContext aerospikeMappingContext,
                                               AerospikeExceptionTranslator aerospikeExceptionTranslator,
                                               QueryEngine queryEngine, IndexRefresher indexRefresher) {
        return new AerospikeTemplate(aerospikeClient, nameSpace(), mappingAerospikeConverter,
            aerospikeMappingContext, aerospikeExceptionTranslator, queryEngine, indexRefresher);
    }

    @Bean(name = "aerospikeQueryEngine")
    public QueryEngine queryEngine(IAerospikeClient aerospikeClient,
                                   StatementBuilder statementBuilder,
                                   FilterExpressionsBuilder filterExpressionsBuilder) {
        QueryEngine queryEngine = new QueryEngine(aerospikeClient, statementBuilder, filterExpressionsBuilder,
            aerospikeClient.getQueryPolicyDefault());
        queryEngine.setScansEnabled(aerospikeDataSettings().isScansEnabled());
        return queryEngine;
    }

    @Bean
    public AerospikePersistenceEntityIndexCreator aerospikePersistenceEntityIndexCreator(
        ObjectProvider<AerospikeMappingContext> aerospikeMappingContext,
        AerospikeIndexResolver aerospikeIndexResolver,
        @Lazy AerospikeTemplate template) {
        return new AerospikePersistenceEntityIndexCreator(aerospikeMappingContext,
            aerospikeDataSettings().isCreateIndexesOnStartup(), aerospikeIndexResolver, template);
    }

    @Bean(name = "aerospikeIndexRefresher")
    public IndexRefresher indexRefresher(IAerospikeClient aerospikeClient, IndexesCacheUpdater indexesCacheUpdater) {
        IndexRefresher refresher = new IndexRefresher(aerospikeClient, aerospikeClient.getInfoPolicyDefault(),
            new InternalIndexOperations(new IndexInfoParser()), indexesCacheUpdater);
        refresher.refreshIndexes();
        return refresher;
    }
}
