package io.github.zomky.storage.log;

import io.github.zomky.storage.RaftStorageConfiguration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Collection;

import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class SegmentsTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentsTest.class);

    @TempDir
    Path directory;

    @Mock
    RaftStorageConfiguration configuration;

    Segments segments;

    @BeforeEach
    void setUp() {
        LOGGER.info("directory {}", directory);
        given(configuration.getSegmentSize()).willReturn(1024);
        given(configuration.getDirectory()).willReturn(directory);
        segments = new Segments(configuration);
    }

    @AfterEach
    void tearDown() {
        segments.release();
    }

    @Test
    void tailSegments() {
        Segment segment2 = segments.createNextSegment(10);
        Segment segment3 = segments.createNextSegment(20);
        Segment segment4 = segments.createNextSegment(30);

        Collection<Segment> actual = segments.getTailSegments(15);

        Assertions.assertThat(actual).doesNotContain(segment2);
        Assertions.assertThat(actual).containsExactly(segment3, segment4);
    }

    @Test
    void tailSegmentsIndexEqualsToSegmentFirstIndex() {
        Segment segment2 = segments.createNextSegment(10);
        Segment segment3 = segments.createNextSegment(20);
        Segment segment4 = segments.createNextSegment(30);

        Collection<Segment> actual = segments.getTailSegments(20);

        Assertions.assertThat(actual).doesNotContain(segment2);
        Assertions.assertThat(actual).containsExactly(segment3, segment4);
    }
}