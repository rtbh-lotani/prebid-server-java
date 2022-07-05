package org.prebid.server.auction;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iab.openrtb.request.App;
import com.iab.openrtb.request.Data;
import com.iab.openrtb.request.Site;
import com.iab.openrtb.request.User;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;
import org.apache.commons.collections4.CollectionUtils;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.json.JsonMerger;
import org.prebid.server.proto.openrtb.ext.request.ExtApp;
import org.prebid.server.proto.openrtb.ext.request.ExtBidderConfig;
import org.prebid.server.proto.openrtb.ext.request.ExtBidderConfigOrtb;
import org.prebid.server.proto.openrtb.ext.request.ExtRequest;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidBidderConfig;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidData;
import org.prebid.server.proto.openrtb.ext.request.ExtSite;
import org.prebid.server.proto.openrtb.ext.request.ExtUser;
import org.prebid.server.proto.request.Targeting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class FpdResolver {

    private static final String USER = "user";
    private static final String SITE = "site";
    private static final String BIDDERS = "bidders";
    private static final String APP = "app";
    private static final Set<String> KNOWN_FPD_ATTRIBUTES = Set.of(USER, SITE, APP, BIDDERS);
    private static final String ALLOW_ALL_BIDDERS = "*";
    private static final String EXT = "ext";
    private static final String CONTEXT = "context";
    private static final String DATA = "data";
    private static final Set<String> USER_DATA_ATTR = Collections.singleton("geo");
    private static final Set<String> APP_DATA_ATTR = Set.of("id", "content", "publisher", "privacypolicy");
    private static final Set<String> SITE_DATA_ATTR = Set.of("id", "content", "publisher", "privacypolicy", "mobile");

    private static final TypeReference<List<String>> STRING_ARRAY_TYPE_REFERENCE =
            new TypeReference<>() {
            };
    private static final TypeReference<List<Data>> DATA_ARRAY_TYPE_REFERENCE =
            new TypeReference<>() {
            };

    private final JacksonMapper jacksonMapper;
    private final JsonMerger jsonMerger;

    public FpdResolver(JacksonMapper jacksonMapper, JsonMerger jsonMerger) {
        this.jacksonMapper = Objects.requireNonNull(jacksonMapper);
        this.jsonMerger = Objects.requireNonNull(jsonMerger);
    }

    public User resolveUser(User originUser, ObjectNode fpdUser) {
        if (fpdUser == null) {
            return originUser;
        }
        final User resultUser = originUser == null ? User.builder().build() : originUser;
        final ExtUser resolvedExtUser = resolveUserExt(fpdUser, resultUser);

        return resultUser.toBuilder()
                .yob(fdpValueOrDefault(fpdUser, FpdFields.YOB, resultUser.getYob()))
                .gender(fdpValueOrDefault(fpdUser, FpdFields.GENDER, resultUser.getGender()))
                .keywords(fdpValueOrDefault(fpdUser, FpdFields.KEYWORDS, resultUser.getKeywords()))
                .data(fdpValueOrDefault(fpdUser, FpdFields.DATA, resultUser.getData()))
                .ext(resolvedExtUser)
                .build();
    }

    private ExtUser resolveUserExt(ObjectNode fpdUser, User originUser) {
        final ExtUser originExtUser = originUser.getExt();
        final ObjectNode resolvedData =
                mergeExtData(fpdUser.path(EXT).path(DATA), originExtUser != null ? originExtUser.getData() : null);

        return updateUserExtDataWithFpdAttr(fpdUser, originExtUser, resolvedData);
    }

    private ExtUser updateUserExtDataWithFpdAttr(ObjectNode fpdUser, ExtUser originExtUser, ObjectNode extData) {
        ObjectNode resultData = extData != null ? extData : jacksonMapper.mapper().createObjectNode();
        USER_DATA_ATTR.forEach(attribute -> setAttr(fpdUser, resultData, attribute));
        return originExtUser != null
                ? originExtUser.toBuilder().data(resultData.isEmpty() ? null : resultData).build()
                : resultData.isEmpty() ? null : ExtUser.builder().data(resultData).build();
    }

    public App resolveApp(App originApp, ObjectNode fpdApp) {
        if (fpdApp == null) {
            return originApp;
        }
        final App resultApp = originApp == null ? App.builder().build() : originApp;
        final ExtApp resolvedExtApp = resolveAppExt(fpdApp, resultApp);

        return resultApp.toBuilder()
                .name(fdpValueOrDefault(fpdApp, FpdFields.NAME, resultApp.getName()))
                .bundle(fdpValueOrDefault(fpdApp, FpdFields.BUNDLE, resultApp.getBundle()))
                .domain(fdpValueOrDefault(fpdApp, FpdFields.DOMAIN, resultApp.getDomain()))
                .storeurl(fdpValueOrDefault(fpdApp, FpdFields.STOREURL, resultApp.getStoreurl()))
                .cat(fdpValueOrDefault(fpdApp, FpdFields.CAT, resultApp.getCat()))
                .sectioncat(fdpValueOrDefault(fpdApp, FpdFields.SECTIONCAT, resultApp.getSectioncat()))
                .pagecat(fdpValueOrDefault(fpdApp, FpdFields.PAGECAT, resultApp.getPagecat()))
                .keywords(fdpValueOrDefault(fpdApp, FpdFields.KEYWORDS, resultApp.getKeywords()))
                .ext(resolvedExtApp)
                .build();
    }

    private ExtApp resolveAppExt(ObjectNode fpdApp, App originApp) {
        final ExtApp originExtApp = originApp.getExt();
        final ObjectNode resolvedData =
                mergeExtData(fpdApp.path(EXT).path(DATA), originExtApp != null ? originExtApp.getData() : null);

        return updateAppExtDataWithFpdAttr(fpdApp, originExtApp, resolvedData);
    }

    private ExtApp updateAppExtDataWithFpdAttr(ObjectNode fpdApp, ExtApp originExtApp, ObjectNode extData) {
        final ObjectNode resultData = extData != null ? extData : jacksonMapper.mapper().createObjectNode();
        APP_DATA_ATTR.forEach(attribute -> setAttr(fpdApp, resultData, attribute));
        return originExtApp != null
                ? ExtApp.of(originExtApp.getPrebid(), resultData.isEmpty() ? null : resultData)
                : resultData.isEmpty() ? null : ExtApp.of(null, resultData);
    }

    public Site resolveSite(Site originSite, ObjectNode fpdSite) {
        if (fpdSite == null) {
            return originSite;
        }
        final Site resultSite = originSite == null ? Site.builder().build() : originSite;
        final ExtSite resolvedExtSite = resolveSiteExt(fpdSite, resultSite);

        return resultSite.toBuilder()
                .name(fdpValueOrDefault(fpdSite, FpdFields.NAME, resultSite.getName()))
                .domain(fdpValueOrDefault(fpdSite, FpdFields.DOMAIN, resultSite.getDomain()))
                .cat(fdpValueOrDefault(fpdSite, FpdFields.CAT, resultSite.getCat()))
                .sectioncat(fdpValueOrDefault(fpdSite, FpdFields.SECTIONCAT, resultSite.getSectioncat()))
                .pagecat(fdpValueOrDefault(fpdSite, FpdFields.PAGECAT, resultSite.getPagecat()))
                .page(fdpValueOrDefault(fpdSite, FpdFields.PAGE, resultSite.getPage()))
                .ref(fdpValueOrDefault(fpdSite, FpdFields.REF, resultSite.getRef()))
                .search(fdpValueOrDefault(fpdSite, FpdFields.SEARCH, resultSite.getSearch()))
                .keywords(fdpValueOrDefault(fpdSite, FpdFields.KEYWORDS, resultSite.getKeywords()))
                .ext(resolvedExtSite)
                .build();
    }

    private ExtSite resolveSiteExt(ObjectNode fpdSite, Site originSite) {
        final ExtSite originExtSite = originSite.getExt();
        final ObjectNode resolvedData =
                mergeExtData(fpdSite.path(EXT).path(DATA), originExtSite != null ? originExtSite.getData() : null);

        return updateSiteExtDataWithFpdAttr(fpdSite, originExtSite, resolvedData);
    }

    private ExtSite updateSiteExtDataWithFpdAttr(ObjectNode fpdSite, ExtSite originExtSite, ObjectNode extData) {
        final ObjectNode resultData = extData != null ? extData : jacksonMapper.mapper().createObjectNode();
        SITE_DATA_ATTR.forEach(attribute -> setAttr(fpdSite, resultData, attribute));
        return originExtSite != null
                ? ExtSite.of(originExtSite.getAmp(), resultData.isEmpty() ? null : resultData)
                : resultData.isEmpty() ? null : ExtSite.of(null, resultData);
    }

    public ObjectNode resolveImpExt(ObjectNode impExt, ObjectNode targeting) {
        if (targeting == null) {
            return impExt;
        }

        KNOWN_FPD_ATTRIBUTES.forEach(targeting::remove);
        if (!targeting.fieldNames().hasNext()) {
            return impExt;
        }

        if (impExt == null) {
            return jacksonMapper.mapper().createObjectNode()
                    .set(DATA, targeting);
        }

        final JsonNode extImpData = impExt.get(DATA);

        final ObjectNode resolvedData = extImpData != null
                ? (ObjectNode) jsonMerger.merge(targeting, extImpData)
                : targeting;

        return impExt.set(DATA, resolvedData);
    }

    /**
     * @param impExt might be modified within method
     */
    public ObjectNode resolveImpExt(ObjectNode impExt, boolean useFirstPartyData) {
        removeOrReplace(impExt, CONTEXT, sanitizeImpExtContext(impExt, useFirstPartyData));
        removeOrReplace(impExt, DATA, sanitizeImpExtData(impExt, useFirstPartyData));

        return impExt;
    }

    private JsonNode sanitizeImpExtContext(ObjectNode originalImpExt, boolean useFirstPartyData) {
        if (!originalImpExt.hasNonNull(CONTEXT)) {
            return null;
        }

        final JsonNode updatedContextNode = originalImpExt.get(CONTEXT).deepCopy();
        if (!useFirstPartyData && updatedContextNode.hasNonNull(DATA)) {
            ((ObjectNode) updatedContextNode).remove(DATA);
        }

        return updatedContextNode.isObject() && updatedContextNode.isEmpty() ? null : updatedContextNode;
    }

    private JsonNode sanitizeImpExtData(ObjectNode impExt, boolean useFirstPartyData) {
        if (!useFirstPartyData) {
            return null;
        }

        final JsonNode contextNode = impExt.hasNonNull(CONTEXT) ? impExt.get(CONTEXT) : null;
        final JsonNode contextDataNode =
                contextNode != null && contextNode.hasNonNull(DATA) ? contextNode.get(DATA) : null;

        final JsonNode dataNode = impExt.get(DATA);

        final boolean dataIsNullOrObject =
                dataNode == null || dataNode.isObject();
        final boolean contextDataIsObject =
                contextDataNode != null && !contextDataNode.isNull() && contextDataNode.isObject();

        final JsonNode mergedDataNode = dataIsNullOrObject && contextDataIsObject
                ? dataNode != null ? jsonMerger.merge(contextDataNode, dataNode) : contextDataNode
                : dataNode;

        if (mergedDataNode != null && !mergedDataNode.isNull()) {
            return mergedDataNode;
        }

        return null;
    }

    private void removeOrReplace(ObjectNode impExt, String field, JsonNode jsonNode) {
        if (jsonNode == null) {
            impExt.remove(field);
        } else {
            impExt.set(field, jsonNode);
        }
    }

    public ExtRequest resolveBidRequestExt(ExtRequest extRequest, Targeting targeting) {
        if (targeting == null) {
            return extRequest;
        }

        final ExtRequestPrebid extRequestPrebid = extRequest != null ? extRequest.getPrebid() : null;
        final ExtRequestPrebidData extRequestPrebidData = extRequestPrebid != null
                ? extRequestPrebid.getData()
                : null;

        final ExtRequestPrebidData resolvedExtRequestPrebidData = resolveExtRequestPrebidData(extRequestPrebidData,
                targeting.getBidders());
        final List<ExtRequestPrebidBidderConfig> resolvedBidderConfig = createAllowedAllBidderConfig(targeting);

        if (resolvedExtRequestPrebidData != null || resolvedBidderConfig != null) {
            final ExtRequestPrebid.ExtRequestPrebidBuilder prebidBuilder = extRequestPrebid != null
                    ? extRequestPrebid.toBuilder()
                    : ExtRequestPrebid.builder();
            return ExtRequest.of(prebidBuilder
                    .data(resolvedExtRequestPrebidData != null
                            ? resolvedExtRequestPrebidData
                            : extRequestPrebidData)
                    .bidderconfig(resolvedBidderConfig)
                    .build());
        }

        return extRequest;
    }

    private ExtRequestPrebidData resolveExtRequestPrebidData(ExtRequestPrebidData data, List<String> fpdBidders) {
        if (CollectionUtils.isEmpty(fpdBidders)) {
            return null;
        }
        final List<String> originBidders = data != null ? data.getBidders() : Collections.emptyList();
        return CollectionUtils.isEmpty(originBidders)
                ? ExtRequestPrebidData.of(fpdBidders, null)
                : ExtRequestPrebidData.of(mergeBidders(fpdBidders, originBidders), null);
    }

    private List<String> mergeBidders(List<String> fpdBidders, List<String> originBidders) {
        final HashSet<String> resolvedBidders = new HashSet<>(originBidders);
        resolvedBidders.addAll(fpdBidders);
        return new ArrayList<>(resolvedBidders);
    }

    private List<ExtRequestPrebidBidderConfig> createAllowedAllBidderConfig(Targeting targeting) {
        final ObjectNode userNode = targeting.getUser();
        final ObjectNode siteNode = targeting.getSite();
        if (userNode == null && siteNode == null) {
            return null;
        }
        final List<String> bidders = Collections.singletonList(ALLOW_ALL_BIDDERS);

        return Collections.singletonList(ExtRequestPrebidBidderConfig.of(bidders,
                ExtBidderConfig.of(null, ExtBidderConfigOrtb.of(siteNode, null, userNode))));
    }

    private ObjectNode mergeExtData(JsonNode fpdData, JsonNode originData) {
        if (fpdData.isMissingNode() || !fpdData.isObject()) {
            return originData != null && originData.isObject() ? ((ObjectNode) originData).deepCopy() : null;
        }

        if (originData != null && originData.isObject()) {
            return (ObjectNode) jsonMerger.merge(fpdData, originData);
        }
        return fpdData.isObject() ? (ObjectNode) fpdData : null;
    }

    private static void setAttr(ObjectNode source, ObjectNode dest, String fieldName) {
        final JsonNode field = source.get(fieldName);
        if (field != null) {
            dest.set(fieldName, field);
        }
    }

    private <T> T fdpValueOrDefault(ObjectNode firstItem,
                                    FpdFields.Field<T> field,
                                    T defaultValue) {

        final T fpdValue = fpdValue(firstItem, field);
        return fpdValue != null ? fpdValue : defaultValue;
    }

    private <T> T fpdValue(ObjectNode firstItem, FpdFields.Field<T> field) {
        final JsonNode valueNode = firstItem.at(field.getJsonPointer());
        final FpdFields.FieldTypes.FieldType<T> fieldType = field.getFieldType();

        return !valueNode.isMissingNode() && fieldType.isCorrectType(valueNode)
                ? fieldType.convert(jacksonMapper, valueNode)
                : null;
    }

    private static <T> List<T> toList(JacksonMapper jacksonMapper,
                                      JsonNode node,
                                      TypeReference<List<T>> listTypeReference) {

        try {
            return jacksonMapper.mapper().convertValue(node, listTypeReference);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static List<String> toListOfStrings(JacksonMapper jacksonMapper, JsonNode node) {
        return toList(jacksonMapper, node, STRING_ARRAY_TYPE_REFERENCE);
    }

    private static List<Data> toListOfData(JacksonMapper jacksonMapper, JsonNode node) {
        return toList(jacksonMapper, node, DATA_ARRAY_TYPE_REFERENCE);
    }

    private static class FpdFields {

        // User
        private static final Field<Integer> YOB = Field.of("/yob", FieldTypes.INTEGER_TYPE);
        private static final Field<String> GENDER = Field.of("/gender", FieldTypes.STRING_TYPE);
        private static final Field<List<Data>> DATA = Field.of("/" + FpdResolver.DATA, FieldTypes.DATA_ARRAY_TYPE);

        // App
        private static final Field<String> BUNDLE = Field.of("/bundle", FieldTypes.STRING_TYPE);
        private static final Field<String> STOREURL = Field.of("/storeurl", FieldTypes.STRING_TYPE);

        // Site
        private static final Field<String> PAGE = Field.of("/page", FieldTypes.STRING_TYPE);
        private static final Field<String> REF = Field.of("/ref", FieldTypes.STRING_TYPE);
        private static final Field<String> SEARCH = Field.of("/search", FieldTypes.STRING_TYPE);

        // Shared
        private static final Field<String> NAME = Field.of("/name", FieldTypes.STRING_TYPE);
        private static final Field<String> DOMAIN = Field.of("/domain", FieldTypes.STRING_TYPE);
        private static final Field<List<String>> CAT = Field.of("/cat", FieldTypes.STRING_ARRAY_TYPE);
        private static final Field<List<String>> SECTIONCAT = Field.of("/sectioncat", FieldTypes.STRING_ARRAY_TYPE);
        private static final Field<List<String>> PAGECAT = Field.of("/pagecat", FieldTypes.STRING_ARRAY_TYPE);
        private static final Field<String> KEYWORDS = Field.of("/keywords", FieldTypes.STRING_TYPE);

        @Value
        private static class Field<T> {

            JsonPointer jsonPointer;

            FieldTypes.FieldType<T> fieldType;

            public static <T> Field<T> of(String path, FieldTypes.FieldType<T> fieldType) {
                return new Field<>(JsonPointer.valueOf(path), fieldType);
            }
        }

        private static class FieldTypes {

            private static final FieldType<Integer> INTEGER_TYPE = FieldType.of(JsonNode::isInt, JsonNode::intValue);
            private static final FieldType<String> STRING_TYPE = FieldType.of(JsonNode::isTextual, JsonNode::textValue);
            private static final FieldType<List<String>> STRING_ARRAY_TYPE =
                    FieldType.of(JsonNode::isArray, FpdResolver::toListOfStrings);
            private static final FieldType<List<Data>> DATA_ARRAY_TYPE =
                    FieldType.of(JsonNode::isArray, FpdResolver::toListOfData);

            @Value(staticConstructor = "of")
            @Getter(AccessLevel.NONE)
            private static class FieldType<T> {

                Predicate<JsonNode> isCorrectType;

                BiFunction<JacksonMapper, JsonNode, T> nodeConverter;

                public static <T> FieldType<T> of(Predicate<JsonNode> isCorrectType,
                                                  Function<JsonNode, T> nodeConverter) {

                    return FieldType.of(isCorrectType, (jacksonMapper, jsonNode) -> nodeConverter.apply(jsonNode));
                }

                public boolean isCorrectType(JsonNode jsonNode) {
                    return isCorrectType.test(jsonNode);
                }

                public T convert(JacksonMapper jacksonMapper, JsonNode jsonNode) {
                    return nodeConverter.apply(jacksonMapper, jsonNode);
                }
            }
        }
    }
}
