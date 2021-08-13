package com.solr.ltr.ranker.normalizer;

/**
 * Interface to normalize the resulting score of a model
 */
public interface Normalizer {
    float normalize(float val);
}
