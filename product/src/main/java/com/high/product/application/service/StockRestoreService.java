package com.high.product.application.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.high.product.application.dto.external.OrderDetailResponse;
import com.high.product.application.dto.external.OrderItemResponse;
import com.high.product.application.dto.kafka.failure.StockRestoreFailMessage;
import com.high.product.application.dto.kafka.request.StockRestoreCommandRequest;
import com.high.product.application.dto.kafka.success.StockRestoreSuccessMessage;
import com.high.product.application.exception.ProductException;
import com.high.product.application.port.DistributedLockPort;
import com.high.product.application.port.OrderQueryPort;
import com.high.product.application.port.RedisCacheEvictPort;
import com.high.product.application.port.SagaDeduplicationPort;
import com.high.product.domain.repository.KafkaOutboxRepository;
import com.high.product.domain.model.Product_Stock;
import com.high.product.domain.repository.StockRepository;
import com.high.product.exception.ProductErrorCode;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class StockRestoreService {

	private final OrderQueryPort orderQueryPort;
	private final StockRepository stockRepository;
	private final DistributedLockPort distributedLockPort;
	private final SagaDeduplicationPort sagaDeduplicationPort;
	private final RedisCacheEvictPort stockCacheEvictPort;
	private final KafkaOutboxRepository kafkaOutboxRepository; // ObjectMapper 제거됨

	public void handleStockRestore(StockRestoreCommandRequest request) {

		// 외부 조회 (락 밖)
		OrderDetailResponse order =
			orderQueryPort.getOrderDetail(request.orderId())
				.getBody()
				.data();

		List<String> lockKeys = order.orderItems().stream()
			.map(item -> "stock:product:" + item.productId())
			.toList();

		// 멀티 락
		distributedLockPort.executeWithMultiLock(lockKeys, () -> {

			log.info("재고 복원 처리 시작 sagaId={}", request.sagaId());

			// 이미 성공 처리된 복원
			if (sagaDeduplicationPort.exists("restore:success:" + request.sagaId())) {
				log.info("이미 복원 성공 sagaId={}, success 재전송", request.sagaId());

				// 아웃박스 PENDING으로 저장 (재전송)
				kafkaOutboxRepository.saveOutboxEvent(
					"stock-restore-success",
					new StockRestoreSuccessMessage(request.sagaId(), request.orderId(), request.userId())
				);
				return;
			}

			// 이미 실패 처리된 복원
			if (sagaDeduplicationPort.exists("restore:fail:" + request.sagaId())) {
				log.info("이미 복원 실패 sagaId={}, fail 재전송", request.sagaId());

				// 아웃박스 PENDING으로 저장 (재전송)
				kafkaOutboxRepository.saveOutboxEvent(
					"stock-restore-fail",
					new StockRestoreFailMessage(request.sagaId(), request.orderId(), "이미 실패 처리된 복원 saga",
						request.userId())
				);
				return;
			}

			// 최초 처리 여부
			if (!sagaDeduplicationPort.tryProcess("restore:processing:" + request.sagaId(), 600)) {
				log.info("복원 처리 중 sagaId={}, skip", request.sagaId());
				return;
			}

			try {
				// 재고 복원 트랜잭션
				for (OrderItemResponse item : order.orderItems()) {
					Product_Stock stock = stockRepository
						.findByProductId(item.productId())
						.orElseThrow(() -> new ProductException(ProductErrorCode.PRODUCT_NOT_FOUND));

					stock.increase(item.quantity());
				}

				// DB 트랜잭션 이후 캐시 무효화
				for (OrderItemResponse item : order.orderItems()) {
					try {
						stockCacheEvictPort.evictStockCacheAfterCommit(item.productId());
					} catch (Exception cacheEx) {
						log.error("캐시 무효화 실패 (Saga는 계속 진행) - productId: {}, error: {}",
							item.productId(), cacheEx.getMessage());
					}
				}

				// 성공 이벤트 + 멱등성 기록
				sagaDeduplicationPort.tryProcess("restore:success:" + request.sagaId(), 600);
				sagaDeduplicationPort.remove("restore:processing:" + request.sagaId());

				// 아웃박스 PENDING으로 저장
				kafkaOutboxRepository.saveOutboxEvent(
					"stock-restore-success",
					new StockRestoreSuccessMessage(request.sagaId(), request.orderId(), request.userId())
				);

				log.info("재고 복원 성공 sagaId={}", request.sagaId());

			} catch (Exception ex) {

				sagaDeduplicationPort.remove("restore:processing:" + request.sagaId());

				log.error("재고 복원 실패 sagaId={}", request.sagaId(), ex);

				// [추가]: 재고 복원 실패 시 아웃박스 이벤트 생성
				sagaDeduplicationPort.tryProcess("restore:fail:" + request.sagaId(), 600);
				kafkaOutboxRepository.saveOutboxEvent(
					"stock-restore-fail",
					new StockRestoreFailMessage(request.sagaId(), request.orderId(), ex.getMessage(), request.userId())
				);

				throw ex;
			} finally {
				// processing 키 제거 (재처리 가능하도록)
				sagaDeduplicationPort.remove("restore:processing:" + request.sagaId());
			}
		});
	}
}