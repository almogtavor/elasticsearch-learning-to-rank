/*
 * Copyright [2017] Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.solr.ltr.ranker.parser;

import com.solr.ltr.feature.FeatureSet;
import com.solr.ltr.ranker.dectree.NaiveAdditiveDecisionTree;
import com.solr.ltr.ranker.dectree.NaiveAdditiveDecisionTree.Node;
import com.solr.ltr.ranker.normalizer.Normalizer;
import com.solr.ltr.ranker.normalizer.Normalizers;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

/**
 * Parse XGBoost models generated by mjolnir (https://gerrit.wikimedia.org/r/search/MjoLniR)
 */
public class XGBoostJsonParser implements LtrRankerParser {
    public static final String TYPE = "model/xgboost+json";

    @Override
    public NaiveAdditiveDecisionTree parse(FeatureSet set, String model) {
        XGBoostDefinition modelDefinition;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, model)
        ) {
            modelDefinition = XGBoostDefinition.parse(parser, set);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot parse model", e);
        }

        Node[] trees = modelDefinition.getTrees(set);
        float[] weights = new float[trees.length];
        // Tree weights are already encoded in outputs
        Arrays.fill(weights, 1F);
        return new NaiveAdditiveDecisionTree(trees, weights, set.size(), modelDefinition.normalizer);
    }

    private static class XGBoostDefinition {
        private static final ObjectParser<XGBoostDefinition, FeatureSet> PARSER;
        static {
            PARSER = new ObjectParser<>("xgboost_definition", XGBoostDefinition::new);
            PARSER.declareString(XGBoostDefinition::setNormalizer, new ParseField("objective"));
            PARSER.declareObjectArray(XGBoostDefinition::setSplitParserStates, SplitParserState::parse, new ParseField("splits"));
        }

        private Normalizer normalizer;
        private List<SplitParserState> splitParserStates;

        public static XGBoostDefinition parse(XContentParser parser, FeatureSet set) throws IOException {
            XGBoostDefinition definition;
            XContentParser.Token startToken = parser.nextToken();

            // The model definition can either be an array of tree definitions, or an object containing the
            // tree definitions in the 'splits' field. Using an object allows for specification of additional
            // parameters.
            if (startToken == XContentParser.Token.START_OBJECT) {
                try {
                    definition = PARSER.apply(parser, set);
                } catch (XContentParseException e) {
                    throw new ParsingException(parser.getTokenLocation(), "Unable to parse XGBoost object", e);
                }
                if (definition.splitParserStates == null) {
                    throw new ParsingException(parser.getTokenLocation(), "XGBoost model missing required field [splits]");
                }
            } else if (startToken == XContentParser.Token.START_ARRAY) {
                definition = new XGBoostDefinition();
                definition.splitParserStates = new ArrayList<>();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    definition.splitParserStates.add(SplitParserState.parse(parser, set));
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Expected [START_ARRAY] or [START_OBJECT] but got ["
                        + startToken + "]");
            }
            if (definition.splitParserStates.size() == 0) {
                throw new ParsingException(parser.getTokenLocation(), "XGBoost model must define at lease one tree");
            }
            return definition;
        }

        XGBoostDefinition() {
            normalizer = Normalizers.get(Normalizers.NOOP_NORMALIZER_NAME);
        }

        /**
         * Set a normalizer based on the 'objective' parameter of the XGBoost model
         *
         * Depending on the objective, the model prediction may require normalization. Currently only
         * untransformed (noop) and logistic (sigmoid) types are implemented.
         * See <a href="https://xgboost.readthedocs.io/en/latest/parameter.html#learning-task-parameters">task params</a>
         *
         * @param objectiveName XGBoost objective name
         */
        void setNormalizer(String objectiveName) {
            switch (objectiveName) {
                case "binary:logitraw":
                case "rank:pairwise":
                case "reg:linear":
                    normalizer = Normalizers.get(Normalizers.NOOP_NORMALIZER_NAME);
                    break;
                case "binary:logistic":
                case "reg:logistic":
                    normalizer = Normalizers.get(Normalizers.SIGMOID_NORMALIZER_NAME);
                    break;
                default:
                    throw new IllegalArgumentException("Objective [" + objectiveName + "] is not a valid XGBoost objective");
            }
        }

        void setSplitParserStates(List<SplitParserState> splitParserStates) {
            this.splitParserStates = splitParserStates;
        }

        Node[] getTrees(FeatureSet set) {
            Node[] trees = new Node[splitParserStates.size()];
            ListIterator<SplitParserState> it = splitParserStates.listIterator();
            while(it.hasNext()) {
                trees[it.nextIndex()] = it.next().toNode(set);
            }
            return trees;
        }
    }

    private static class SplitParserState {
        private static final ObjectParser<SplitParserState, FeatureSet> PARSER;
        static {
            PARSER = new ObjectParser<>("node", SplitParserState::new);
            PARSER.declareInt(SplitParserState::setNodeId, new ParseField("nodeid"));
            PARSER.declareInt(SplitParserState::setDepth, new ParseField("depth"));
            PARSER.declareString(SplitParserState::setSplit, new ParseField("split"));
            PARSER.declareFloat(SplitParserState::setThreshold, new ParseField("split_condition"));
            PARSER.declareInt(SplitParserState::setRightNodeId, new ParseField("no"));
            PARSER.declareInt(SplitParserState::setLeftNodeId, new ParseField("yes"));
            PARSER.declareInt(SplitParserState::setMissingNodeId, new ParseField("missing"));
            PARSER.declareFloat(SplitParserState::setLeaf, new ParseField("leaf"));
            PARSER.declareObjectArray(SplitParserState::setChildren, SplitParserState::parse,
                    new ParseField("children"));
            PARSER.declareFloat(SplitParserState::setThreshold, new ParseField("split_condition"));
        }

        private Integer nodeId;
        private Integer depth;
        private String split;
        private Float threshold;
        private Integer rightNodeId;
        private Integer leftNodeId;
        // Ignored
        private Integer missingNodeId;
        private Float leaf;
        private List<SplitParserState> children;

        public static SplitParserState parse(XContentParser parser, FeatureSet set) {
            SplitParserState split = PARSER.apply(parser, set);
            if (split.isSplit()) {
                if (!split.splitHasAllFields()) {
                    throw new ParsingException(parser.getTokenLocation(), "This split does not have all the required fields");
                }
                if (!split.splitHasValidChildren()) {
                    throw new ParsingException(parser.getTokenLocation(), "Split structure is invalid, yes, no and/or" +
                            " missing branches does not point to the proper children.");
                }
                if (!set.hasFeature(split.split)) {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown feature [" + split.split + "]");
                }
            } else if (!split.leafHasAllFields()) {
                throw new ParsingException(parser.getTokenLocation(), "This leaf does not have all the required fields");
            }
            return split;
        }
        void setNodeId(Integer nodeId) {
            this.nodeId = nodeId;
        }

        void setDepth(Integer depth) {
            this.depth = depth;
        }

        void setSplit(String split) {
            this.split = split;
        }

        void setThreshold(Float threshold) {
            this.threshold = threshold;
        }

        void setRightNodeId(Integer rightNodeId) {
            this.rightNodeId = rightNodeId;
        }

        void setLeftNodeId(Integer leftNodeId) {
            this.leftNodeId = leftNodeId;
        }

        void setMissingNodeId(Integer missingNodeId) {
            this.missingNodeId = missingNodeId;
        }

        void setLeaf(Float leaf) {
            this.leaf = leaf;
        }

        void setChildren(List<SplitParserState> children) {
            this.children = children;
        }

        boolean splitHasAllFields() {
            return nodeId != null && threshold != null && split != null && leftNodeId != null && rightNodeId != null && depth != null
                    && children != null && children.size() == 2;
        }

        boolean leafHasAllFields() {
            return nodeId != null && leaf != null;
        }

        boolean splitHasValidChildren() {
            return children.size() == 2 &&
                    leftNodeId.equals(children.get(0).nodeId) && rightNodeId.equals(children.get(1).nodeId);
        }
        boolean isSplit() {
            return leaf == null;
        }


        Node toNode(FeatureSet set) {
            if (isSplit()) {
                return new NaiveAdditiveDecisionTree.Split(children.get(0).toNode(set), children.get(1).toNode(set),
                        set.featureOrdinal(split), threshold);
            } else {
                return new NaiveAdditiveDecisionTree.Leaf(leaf);
            }
        }
    }
}