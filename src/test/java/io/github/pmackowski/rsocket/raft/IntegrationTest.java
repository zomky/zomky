package io.github.pmackowski.rsocket.raft;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({ TYPE, METHOD, ANNOTATION_TYPE })
@Retention(RUNTIME)
@Test
@Tag("integration")
public @interface IntegrationTest { }
