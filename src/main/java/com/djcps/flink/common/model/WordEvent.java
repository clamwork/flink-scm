package com.djcps.flink.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class WordEvent {
    private String word;
    private int count;
    private long timestamp;
}
