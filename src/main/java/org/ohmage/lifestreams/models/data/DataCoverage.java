package org.ohmage.lifestreams.models.data;

import org.joda.time.DateTime;

/**
 * Created by changun on 6/20/14.
 */
public class DataCoverage {
    DateTime begin, end;

    Double coverage;
    public DateTime getBegin() {
        return begin;
    }

    public void setBegin(DateTime begin) {
        this.begin = begin;
    }

    public DateTime getEnd() {
        return end;
    }

    public void setEnd(DateTime end) {
        this.end = end;
    }

    public Double getCoverage() {
        return coverage;
    }

    public void setCoverage(Double coverage) {
        this.coverage = coverage;
    }
    public DataCoverage(){};

    public DataCoverage(DateTime begin, DateTime end, Double coverage) {
        this.begin = begin;
        this.end = end;
        if(coverage > 1.1){
            throw  new RuntimeException("coverage cannot be over 1.0 too much, given coverage=" + coverage);
        }else if(coverage > 1.0){
            coverage = 1.0;
        }
        this.coverage = coverage;
    }
}
