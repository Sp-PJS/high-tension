package com.high.product.infrastructure.redis;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec; // [추가] Jackson 코덱
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class RedissonConfig {

	private final RedissonProperties properties;

	@Bean(destroyMethod = "shutdown")
	public RedissonClient redissonClient() {

		Config config = new Config();

		// 1. [추가] Spring Cache(RedisCacheConfig)와 동일한 ObjectMapper 설정 생성
		// Redisson이 멱등성 키(success:sagaId)를 저장할 때도 같은 포맷을 쓰게 하여 직렬화 에러를 방지합니다.
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.registerModule(new JavaTimeModule());
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
			.allowIfBaseType(Object.class)
			.build();

		objectMapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);

		// 2. [추가] Redisson 코덱 설정
		config.setCodec(new JsonJacksonCodec(objectMapper));

		config.setLockWatchdogTimeout(60000);

		// cluster 모드라면 cluster 환경 분산 락
		if ("cluster".equalsIgnoreCase(properties.getMode())) {

			config.useClusterServers()
				.addNodeAddress(
					properties.getCluster()
						.getNodes()
						.toArray(String[]::new)
				)
				.setScanInterval(2000)
				.setTimeout(3000)
				.setRetryAttempts(3)
				.setRetryInterval(1500);

			// 아니라면 단일서버로 동작
		} else {

			config.useSingleServer()
				.setAddress(properties.getSingle().getAddress())
				.setConnectionMinimumIdleSize(5)
				.setConnectionPoolSize(20)
				.setTimeout(3000)
				.setRetryAttempts(3)
				.setRetryInterval(1500);
		}

		return Redisson.create(config);
	}
}
