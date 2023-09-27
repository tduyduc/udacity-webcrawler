package com.udacity.webcrawler.profiler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {

  private final Clock clock;
  private final ProfilingState state;
  private final Object target;

  ProfilingMethodInterceptor(
    Clock clock,
    ProfilingState state,
    Object target
  ) {
    this.clock = Objects.requireNonNull(clock);
    this.state = Objects.requireNonNull(state);
    this.target = Objects.requireNonNull(target);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    final Runnable recordState;
    if (null == method.getAnnotation(Profiled.class)) {
      recordState = () -> {};
    } else {
      final ZonedDateTime startTime = ZonedDateTime.now(this.clock);
      recordState = () -> this.state.record(
        target.getClass(),
        method,
        Duration.between(startTime, ZonedDateTime.now(this.clock))
      );
    }

    try {
      return method.invoke(this.target, args);
    } catch (InvocationTargetException ite) {
      throw ite.getTargetException();
    } catch (IllegalAccessException | UndeclaredThrowableException e) {
      throw new RuntimeException(e);
    } finally {
      recordState.run();
    }
  }
}
