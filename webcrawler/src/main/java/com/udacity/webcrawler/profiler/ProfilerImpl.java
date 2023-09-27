package com.udacity.webcrawler.profiler;

import javax.inject.Inject;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

/**
 * Concrete implementation of the {@link Profiler}.
 */
final class ProfilerImpl implements Profiler {

  private final Clock clock;
  private final ProfilingState state = new ProfilingState();
  private final ZonedDateTime startTime;

  @Inject
  ProfilerImpl(Clock clock) {
    this.clock = Objects.requireNonNull(clock);
    this.startTime = ZonedDateTime.now(clock);
  }

  @Override
  public <T> T wrap(Class<T> klass, T delegate) {
    Objects.requireNonNull(klass);

    final InvocationHandler invocationHandler = new ProfilingMethodInterceptor(
      this.clock,
      this.state,
      delegate
    );

    if (Arrays.stream(klass.getMethods()).noneMatch(
      method -> null != method.getAnnotation(Profiled.class)
    )) {
      throw new IllegalArgumentException();
    }

    return (T) Proxy.newProxyInstance(
      klass.getClassLoader(),
      new Class[] { klass },
      invocationHandler
    );
  }

  @Override
  public void writeData(Path path) throws IOException {
    try (final BufferedWriter bw = Files.newBufferedWriter(
      path,
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND
    )) {
      this.writeData(bw);
    }
  }

  @Override
  public void writeData(Writer writer) throws IOException {
    writer.write("Run at " + RFC_1123_DATE_TIME.format(startTime));
    writer.write(System.lineSeparator());
    state.write(writer);
    writer.write(System.lineSeparator());
  }
}
