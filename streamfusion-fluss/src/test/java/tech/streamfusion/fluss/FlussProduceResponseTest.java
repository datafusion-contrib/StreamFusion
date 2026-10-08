package tech.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.junit.jupiter.api.Test;

class FlussProduceResponseTest {
  @Test
  void previouslyCommittedDuplicateIsAcknowledgedLikeTheSdk() {
    assertDoesNotThrow(() -> FlussArrowClient.validateAppendResponse(response(3, Errors.NONE), 3));
    assertDoesNotThrow(
        () ->
            FlussArrowClient.validateAppendResponse(
                response(3, Errors.DUPLICATE_SEQUENCE_EXCEPTION), 3));
  }

  @Test
  void duplicatesCannotHideTheWrongBucketOrMultipleResponses() {
    assertThrows(
        IOException.class,
        () ->
            FlussArrowClient.validateAppendResponse(
                response(2, Errors.DUPLICATE_SEQUENCE_EXCEPTION), 3));
    var multiple = response(3, Errors.DUPLICATE_SEQUENCE_EXCEPTION);
    multiple.addBucketsResp().setBucketId(3);
    assertThrows(IOException.class, () -> FlussArrowClient.validateAppendResponse(multiple, 3));
  }

  @Test
  void duplicateAcknowledgementsCannotCrossPartitionsWithTheSameBucketNumber() {
    var bucket = new org.apache.fluss.metadata.TableBucket(1, 42L, 3);
    var reply = response(3, Errors.DUPLICATE_SEQUENCE_EXCEPTION);
    reply.getBucketsRespAt(0).setPartitionId(42);
    assertDoesNotThrow(() -> FlussArrowClient.validateAppendResponse(reply, bucket));
    reply.getBucketsRespAt(0).setPartitionId(43);
    assertThrows(IOException.class, () -> FlussArrowClient.validateAppendResponse(reply, bucket));
    reply.getBucketsRespAt(0).clearPartitionId();
    assertThrows(IOException.class, () -> FlussArrowClient.validateAppendResponse(reply, bucket));
  }

  @Test
  void authorizationAndLostSequencesStillFailTheWriter() {
    assertThrows(
        AuthorizationException.class,
        () ->
            FlussArrowClient.validateAppendResponse(
                response(3, Errors.AUTHORIZATION_EXCEPTION), 3));
    assertThrows(
        OutOfOrderSequenceException.class,
        () ->
            FlussArrowClient.validateAppendResponse(
                response(3, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION), 3));
  }

  private static ProduceLogResponse response(int bucket, Errors error) {
    var reply = new ProduceLogResponse();
    reply.addBucketsResp().setBucketId(bucket).setErrorCode(error.code());
    return reply;
  }
}
