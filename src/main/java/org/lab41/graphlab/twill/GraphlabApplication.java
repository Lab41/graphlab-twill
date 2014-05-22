package org.lab41.graphlab.twill;

import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

/**
 * Created by etryzelaar on 5/20/14.
 */
public class GraphlabApplication implements TwillApplication {

    String zkStr;
    ResourceSpecification resource;

    public GraphlabApplication(String zkStr, ResourceSpecification resource) {
        this.zkStr = zkStr;
        this.resource = resource;
    }

    @Override
    public TwillSpecification configure() {
        TwillSpecification.Builder.MoreRunnable x = TwillSpecification.Builder.with()
                .setName("GraphlabApplication")
                .withRunnable()
                .add(0, new GraphlabRunnable(zkStr)).noLocalFiles();

        for (int i = 1; i < instances; ++i) {

        }

        x.add()
    }
}
