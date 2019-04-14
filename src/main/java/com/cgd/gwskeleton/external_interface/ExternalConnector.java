package com.cgd.gwskeleton.external_interface;

import com.cgd.gwskeleton.publisher.DataPublisher;

public interface ExternalConnector {
    public void start(DataPublisher publisher);
}
