package de.codecentric.kafka.demo.embeddedkafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.MethodCallback;
import org.springframework.util.ReflectionUtils.MethodFilter;

/**
 * Utilities for testing listener containers. No hard references to container
 * classes are used to avoid circular project dependencies.
 *
 * @author Gary Russell
 * @since 1.0.3
 */
public final class ContainerTestUtils {

	private ContainerTestUtils() {
		// private ctor
	}

	/**
	 * Wait until the container has the required number of assigned partitions.
	 * @param container the container.
	 * @param partitions the number of partitions.
	 * @throws Exception an exception.
	 */
	public static void waitForAssignment(Object container, int partitions)
			throws Exception {
		if (container.getClass().getSimpleName().contains("KafkaMessageListenerContainer")) {
			waitForSingleContainerAssignment(container, partitions);
			return;
		}
		List<?> containers = KafkaTestUtils.getPropertyValue(container, "containers", List.class);
		int n = 0;
		int count = 0;
		Method getAssignedPartitions = null;
		while (n++ < 600 && count < partitions) {
			count = 0;
			for (Object aContainer : containers) {
				if (getAssignedPartitions == null) {
					getAssignedPartitions = getAssignedPartitionsMethod(aContainer.getClass());
				}
				Collection<?> assignedPartitions = (Collection<?>) getAssignedPartitions.invoke(aContainer);
				if (assignedPartitions != null) {
					count += assignedPartitions.size();
				}
			}
			if (count < partitions) {
				Thread.sleep(100);
			}
		}
		assertThat(count).isEqualTo(partitions);
	}

	private static void waitForSingleContainerAssignment(Object container, int partitions)
			throws Exception {
		int n = 0;
		int count = 0;
		Method getAssignedPartitions = getAssignedPartitionsMethod(container.getClass());
		while (n++ < 600 && count < partitions) {
			count = 0;
			Collection<?> assignedPartitions = (Collection<?>) getAssignedPartitions.invoke(container);
			if (assignedPartitions != null) {
				count = assignedPartitions.size();
			}
			if (count < partitions) {
				Thread.sleep(100);
			}
		}
		assertThat(count).isEqualTo(partitions);
	}

	private static Method getAssignedPartitionsMethod(Class<?> clazz) {
		final AtomicReference<Method> theMethod = new AtomicReference<Method>();
		ReflectionUtils.doWithMethods(clazz, new MethodCallback() {

			@Override
			public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
				theMethod.set(method);
			}
		}, new MethodFilter() {

			@Override
			public boolean matches(Method method) {
				return method.getName().equals("getAssignedPartitions") && method.getParameterTypes().length == 0;
			}
		});
		assertThat(theMethod.get()).isNotNull();
		return theMethod.get();
	}

}
