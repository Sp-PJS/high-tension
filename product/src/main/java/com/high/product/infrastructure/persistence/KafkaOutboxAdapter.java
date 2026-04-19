package com.high.product.infrastructure.persistence;

import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.high.product.domain.model.KafkaOutbox;
import com.high.product.domain.repository.KafkaOutboxRepository;

import com.high.product.application.dto.kafka.success.StockDeductionSuccessMessage;
import com.high.product.application.dto.kafka.failure.StockDeductionFailMessage;
import com.high.product.application.dto.kafka.success.StockRestoreSuccessMessage;
import com.high.product.application.dto.kafka.failure.StockRestoreFailMessage;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaOutboxAdapter implements KafkaOutboxRepository {

	private final JpaKafkaOutboxRepository jpaKafkaOutboxRepository;
	private final ObjectMapper objectMapper;

	@Override
	public List<KafkaOutbox> findTop100ByStatusOrderByCreatedAtAsc(String status) {
		return jpaKafkaOutboxRepository.findTop100ByStatusOrderByCreatedAtAsc(status);
	}

	@Override
	public KafkaOutbox save(KafkaOutbox outbox) {
		return jpaKafkaOutboxRepository.save(outbox);
	}

	@Override
	public void markAsSent(KafkaOutbox outbox) {
		// 1. 엔티티 내부의 상태 변경 메서드 호출 (비즈니스 로직)
		outbox.markSent();
		// 2. 변경된 엔티티를 DB에 저장
		jpaKafkaOutboxRepository.save(outbox);
	}

	@Override
    public void saveOutboxEvent(String topic, Object message) {
        try {
            String payload = objectMapper.writeValueAsString(message);
            KafkaOutbox outbox = KafkaOutbox.builder()
                .topic(topic)
                .messageKey(extractMessageKey(message))
                .payload(payload)
                .status("PENDING")
                .sagaId(extractSagaId(message))
                .orderId(extractOrderId(message))
                .userId(extractUserId(message))
                .build();

            jpaKafkaOutboxRepository.save(outbox);
        } catch (Exception e) {
            log.error("Kafka Outbox 저장 실패", e);
            throw new RuntimeException(e);
        }
    }

    private String extractMessageKey(Object m) {
        if (m instanceof StockDeductionSuccessMessage s) return String.valueOf(s.sagaId());
        if (m instanceof StockDeductionFailMessage f) return String.valueOf(f.sagaId());
        if (m instanceof StockRestoreSuccessMessage s) return String.valueOf(s.sagaId());
        if (m instanceof StockRestoreFailMessage f) return String.valueOf(f.sagaId());
        return UUID.randomUUID().toString();
    }

    private UUID extractSagaId(Object m) {
        if (m instanceof StockDeductionSuccessMessage s) return s.sagaId();
        if (m instanceof StockDeductionFailMessage f) return f.sagaId();
        if (m instanceof StockRestoreSuccessMessage s) return s.sagaId();
        if (m instanceof StockRestoreFailMessage f) return f.sagaId();
        return null;
    }

    private UUID extractOrderId(Object m) {
        if (m instanceof StockDeductionSuccessMessage s) return s.orderId();
        if (m instanceof StockDeductionFailMessage f) return f.orderId();
        if (m instanceof StockRestoreSuccessMessage s) return s.orderId();
        if (m instanceof StockRestoreFailMessage f) return f.orderId();
        return null;
    }

    private UUID extractUserId(Object m) {
        if (m instanceof StockDeductionSuccessMessage s) return s.userId();
        if (m instanceof StockDeductionFailMessage f) return f.userId();
        if (m instanceof StockRestoreSuccessMessage s) return s.userId();
        if (m instanceof StockRestoreFailMessage f) return f.userId();
        return null;
    }
}
