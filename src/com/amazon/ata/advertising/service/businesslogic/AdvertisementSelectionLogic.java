package com.amazon.ata.advertising.service.businesslogic;

import com.amazon.ata.advertising.service.dao.ReadableDao;
import com.amazon.ata.advertising.service.model.AdvertisementContent;
import com.amazon.ata.advertising.service.model.EmptyGeneratedAdvertisement;
import com.amazon.ata.advertising.service.model.GeneratedAdvertisement;
import com.amazon.ata.advertising.service.model.RequestContext;
import com.amazon.ata.advertising.service.targeting.TargetingEvaluator;
import com.amazon.ata.advertising.service.targeting.TargetingGroup;

import com.amazon.ata.advertising.service.targeting.predicate.TargetingPredicateResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;

import static java.util.Arrays.stream;

/**
 * This class is responsible for picking the advertisement to be rendered.
 */
public class AdvertisementSelectionLogic {

    private static final Logger LOG = LogManager.getLogger(AdvertisementSelectionLogic.class);

    private final ReadableDao<String, List<AdvertisementContent>> contentDao;
    private final ReadableDao<String, List<TargetingGroup>> targetingGroupDao;
    private Random random = new Random();


    /**
     * Constructor for AdvertisementSelectionLogic.
     * @param contentDao Source of advertising content.
     * @param targetingGroupDao Source of targeting groups for each advertising content.
     */
    @Inject
    public AdvertisementSelectionLogic(ReadableDao<String, List<AdvertisementContent>> contentDao,
                                       ReadableDao<String, List<TargetingGroup>> targetingGroupDao) {
        this.contentDao = contentDao;
        this.targetingGroupDao = targetingGroupDao;

    }

    /**
     * Setter for Random class.
     * @param random generates random number used to select advertisements.
     */
    public void setRandom(Random random) {
        this.random = random;
    }

    /**
     * Gets all of the content and metadata for the marketplace and determines which content can be shown.  Returns the
     * eligible content with the highest click through rate.  If no advertisement is available or eligible, returns an
     * EmptyGeneratedAdvertisement.
     *
     * @param customerId - the customer to generate a custom advertisement for
     * @param marketplaceId - the id of the marketplace the advertisement will be rendered on
     * @return an advertisement customized for the customer id provided, or an empty advertisement if one could
     *     not be generated.
     */
    public GeneratedAdvertisement selectAdvertisement(String customerId, String marketplaceId) {

        TargetingEvaluator targetingEvaluator = new TargetingEvaluator(new RequestContext(customerId, marketplaceId));
         final List<AdvertisementContent> contents = contentDao.get(marketplaceId);


        if (CollectionUtils.isEmpty(contents)) {
            LOG.info("No advertisements available for marketplaceId: " + marketplaceId);
            return new EmptyGeneratedAdvertisement();
        }


        final List<AdvertisementContent> eligibleAds = contents.stream()
                .flatMap(content -> Optional.ofNullable(content)
                        .map(c -> {

                            List<TargetingGroup> targetingGroups = Optional.ofNullable(targetingGroupDao.get(c.getContentId()))
                                    .orElse(Collections.emptyList());
                            //i'm sorting using stream so as not to modify the list directly

                            return targetingGroups.stream()
                                    .sorted(Comparator.comparingDouble(TargetingGroup::getClickThroughRate).reversed())
                                    .filter(Objects::nonNull)
                                    .filter(targetingGroup -> Optional.ofNullable(targetingEvaluator.evaluate(targetingGroup))
                                            .map(TargetingPredicateResult::isTrue)
                                            .orElse(false))
                                    .map(targetingGroup -> c);
                        })
                        .orElseGet(Stream::empty))
                .distinct()
                .collect(Collectors.toList());



        if (eligibleAds.isEmpty()) {
            LOG.info("No eligible advertisements found for customerId: " + customerId + ", marketplaceId: " + marketplaceId);
            return new EmptyGeneratedAdvertisement();
        }

        AdvertisementContent randomAd = eligibleAds.get(random.nextInt(eligibleAds.size()));

        return new GeneratedAdvertisement(randomAd);
    }
}
