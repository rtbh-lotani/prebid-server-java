package org.prebid.server.bidder.rtbhouse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.iab.openrtb.request.Banner;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.response.Bid;
import com.iab.openrtb.response.SeatBid;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.VertxTest;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderCall;
import org.prebid.server.bidder.model.BidderError;
import org.prebid.server.bidder.model.CompositeBidderResponse;
import org.prebid.server.bidder.model.HttpRequest;
import org.prebid.server.bidder.model.HttpResponse;
import org.prebid.server.bidder.model.Result;
import org.prebid.server.bidder.rtbhouse.proto.RtbhouseBidResponse;
import org.prebid.server.bidder.rtbhouse.proto.RtbhouseBidResponseExt;
import org.prebid.server.currency.CurrencyConversionService;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.proto.openrtb.ext.ExtPrebid;
import org.prebid.server.proto.openrtb.ext.request.rtbhouse.ExtImpRtbhouse;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.proto.openrtb.ext.response.FledgeAuctionConfig;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.prebid.server.proto.openrtb.ext.response.BidType.banner;

public class RtbhouseBidderTest extends VertxTest {

    private static final String ENDPOINT_URL = "https://test.endpoint.com";

    private RtbhouseBidder rtbhouseBidder;

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private CurrencyConversionService currencyConversionService;

    @Before
    public void setUp() {
        rtbhouseBidder = new RtbhouseBidder(ENDPOINT_URL, currencyConversionService, jacksonMapper);
    }

    @Test
    public void creationShouldFailOnInvalidEndpointUrl() {
        assertThatIllegalArgumentException().isThrownBy(() -> new RtbhouseBidder(
                "invalid_url",
                currencyConversionService,
                jacksonMapper));
    }

    @Test
    public void makeBidsShouldReturnErrorIfResponseBodyCouldNotBeParsed() {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(null, "invalid");

        // when
        final CompositeBidderResponse result = rtbhouseBidder.makeBidderResponse(httpCall,
                BidRequest.builder().build());

        // then
        assertThat(result.getErrors()).hasSize(1)
                .allMatch(error -> error.getType() == BidderError.Type.bad_server_response
                           && error.getMessage().startsWith("Failed to decode: Unrecognized token 'invalid'"));
        assertThat(result.getBids()).isEmpty();
    }

    @Test
    public void makeBidsShouldReturnEmptyListIfBidResponseIsNull() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(null,
                mapper.writeValueAsString(null));

        // when
        final CompositeBidderResponse result = rtbhouseBidder.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getBids()).isEmpty();
    }

    @Test
    public void makeBidsShouldReturnEmptyListIfBidResponseSeatBidIsNull() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(null,
                mapper.writeValueAsString(RtbhouseBidResponse.builder().build()));

        // when
        final CompositeBidderResponse result = rtbhouseBidder.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getBids()).isEmpty();
    }

    @Test
    public void makeBidsShouldReturnBannerBidIfBannerIsPresent() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                BidRequest.builder()
                        .imp(singletonList(Imp.builder().id("123").build()))
                        .build(),
                mapper.writeValueAsString(
                        givenBidResponse(bidBuilder -> bidBuilder.impid("123"))));

        // when
        final CompositeBidderResponse result = rtbhouseBidder.makeBidderResponse(httpCall, null);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getBids())
                .containsOnly(BidderBid.of(Bid.builder().impid("123").build(), banner, "USD"));
    }

    @Test
    public void makeHttpRequestsShouldConvertCurrencyIfRequestCurrencyDoesNotMatchBidderCurrency() {
        // given
        given(currencyConversionService.convertCurrency(any(), any(), anyString(), anyString()))
                .willReturn(BigDecimal.TEN);

        final BidRequest bidRequest = BidRequest.builder()
                .imp(singletonList(Imp.builder()
                        .bidfloor(BigDecimal.ONE).bidfloorcur("EUR")
                        .ext(mapper.valueToTree(ExtPrebid.of(null, mapper.createObjectNode())))
                        .build()))
                .id("request_id")
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = rtbhouseBidder.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue()).hasSize(1)
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getBidfloor, Imp::getBidfloorcur)
                .containsOnly(tuple(BigDecimal.TEN, "USD"));
    }

    @Test
    public void makeHttpRequestsShouldTakePriceFloorsWhenBidfloorParamIsAlsoPresent() {
        // given

        final BidRequest bidRequest = BidRequest.builder()
                .imp(singletonList(Imp.builder()
                        .bidfloor(BigDecimal.TEN).bidfloorcur("USD")
                        .ext(mapper.valueToTree(ExtPrebid.of(null,
                                ExtImpRtbhouse.builder().bidFloor(BigDecimal.ONE).build())))
                        .build()))
                .id("request_id")
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = rtbhouseBidder.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue()).hasSize(1)
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getBidfloor, Imp::getBidfloorcur)
                .containsOnly(tuple(BigDecimal.TEN, "USD"));
    }

    @Test
    public void makeHttpRequestsShouldTakeBidfloorExtImpParamIfNoBidfloorInRequest() {
        // given

        final BidRequest bidRequest = BidRequest.builder()
                .imp(singletonList(Imp.builder()
                        .ext(mapper.valueToTree(ExtPrebid.of(null,
                                ExtImpRtbhouse.builder().bidFloor(BigDecimal.valueOf(16)).build())))
                        .build()))
                .id("request_id")
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = rtbhouseBidder.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getValue()).hasSize(1)
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getBidfloor)
                .containsExactly(BigDecimal.valueOf(16));
    }

    @Test
    public void makeHttpRequestsShouldReturnErrorMessageOnFailedCurrencyConversion() {
        // given
        given(currencyConversionService.convertCurrency(any(), any(), anyString(), anyString()))
                .willThrow(PreBidException.class);

        final BidRequest bidRequest = BidRequest.builder()
                .imp(singletonList(Imp.builder()
                        .id("123")
                        .bidfloor(BigDecimal.ONE).bidfloorcur("EUR")
                        .ext(mapper.valueToTree(ExtPrebid.of(null, mapper.createObjectNode())))
                        .build()))
                .id("request_id")
                .build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = rtbhouseBidder.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getErrors()).allSatisfy(bidderError -> {
            assertThat(bidderError.getType())
                    .isEqualTo(BidderError.Type.bad_input);
            assertThat(bidderError.getMessage())
                    .isEqualTo("Unable to convert provided bid floor currency from EUR to USD for imp `123`");
        });
    }

    @Test
    public void makeHttpRequestShouldReturnResultWithAuctionEnvironment() {
        // given
        final BidRequest bidRequest = BidRequest.builder()
                .imp(singletonList(
                        Imp.builder()
                                .id("impId2")
                                .banner(Banner.builder().build())
                                .tagid("unitId")
                                .ext(mapper.valueToTree(Map.of("ae", 1, "bidder", Map.of())))
                                .build())
                ).build();

        // when
        final Result<List<HttpRequest<BidRequest>>> result = rtbhouseBidder.makeHttpRequests(bidRequest);

        // then
        assertThat(result.getValue()).hasSize(1)
                .extracting(HttpRequest::getPayload)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .contains(mapper.valueToTree(Map.of("ae", 1)));
    }

    @Test
    public void makeBidsShouldReturnResultWithExpectedFields() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                BidRequest.builder()
                        .imp(singletonList(Imp.builder().id("123").build()))
                        .build(),
                mapper.writeValueAsString(RtbhouseBidResponse.builder()
                .seatbid(singletonList(SeatBid.builder()
                        .bid(singletonList(Bid.builder()
                                .w(200)
                                .h(150)
                                .price(BigDecimal.ONE)
                                .impid("impId1")
                                .dealid("dealid")
                                .adm("<div>This is an Ad</div>")
                                .build()))
                        .build()))
                .cur("UAH")
                .ext(RtbhouseBidResponseExt.of(Map.of("impId1",
                        mapper.createObjectNode().put("somevalue", 1))))
                .build()));

        final BidRequest bidRequest = BidRequest.builder()
                .id("bidRequestId")
                .imp(singletonList(Imp.builder()
                        .id("impId1")
                        .banner(Banner.builder().build())
                        .build()))
                .build();

        // when
        final CompositeBidderResponse result = rtbhouseBidder.makeBidderResponse(httpCall, bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getBids()).hasSize(1)
                .containsOnly(BidderBid.of(
                        Bid.builder()
                                .impid("impId1")
                                .price(BigDecimal.ONE)
                                .dealid("dealid")
                                .w(200)
                                .h(150)
                                .adm("<div>This is an Ad</div>")
                                .build(),
                        BidType.banner, "UAH"));
        assertThat(result.getFledgeAuctionConfigs())
                .containsOnly(FledgeAuctionConfig.builder()
                        .impId("impId1")
                        .config(mapper.createObjectNode().put("somevalue", 1))
                        .build());
    }

    @Test
    public void makeBidsShouldReturnFledgeConfigEvenIfNoBids() throws JsonProcessingException {
        // given
        final BidderCall<BidRequest> httpCall = givenHttpCall(
                BidRequest.builder()
                        .imp(singletonList(Imp.builder().id("123").build()))
                        .build(),
                mapper.writeValueAsString(RtbhouseBidResponse.builder()
                .seatbid(emptyList())
                .ext(RtbhouseBidResponseExt.of(Map.of("impId1",
                        mapper.createObjectNode().put("somevalue", 1))))
                .build()));

        final BidRequest bidRequest = BidRequest.builder()
                .id("bidRequestId")
                .imp(singletonList(Imp.builder()
                        .id("impId1")
                        .banner(Banner.builder().build())
                        .build()))
                .build();

        // when
        final CompositeBidderResponse result = rtbhouseBidder.makeBidderResponse(httpCall, bidRequest);

        // then
        assertThat(result.getErrors()).isEmpty();
        assertThat(result.getBids()).isEmpty();
        assertThat(result.getFledgeAuctionConfigs())
                .containsOnly(FledgeAuctionConfig.builder()
                        .impId("impId1")
                        .config(mapper.createObjectNode().put("somevalue", 1))
                        .build());
    }

    private static RtbhouseBidResponse givenBidResponse(Function<Bid.BidBuilder, Bid.BidBuilder> bidCustomizer) {
        return RtbhouseBidResponse.builder()
                .cur("USD")
                .seatbid(singletonList(SeatBid.builder()
                        .bid(singletonList(bidCustomizer.apply(Bid.builder()).build()))
                        .build()))
                .build();
    }

    private static BidderCall<BidRequest> givenHttpCall(BidRequest bidRequest, String body) {
        return BidderCall.succeededHttp(
                HttpRequest.<BidRequest>builder().payload(bidRequest).build(),
                HttpResponse.of(200, null, body),
                null);
    }
}
