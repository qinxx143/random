package cris.dynamic.backup.system;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class Helper {

    public static <T> ArrayList<T> cloneArrayList(final ArrayList<T> toClone) {
        final ArrayList<T> copy = new ArrayList<T>(toClone.size());

        for (final T element : toClone) {
            copy.add(element);
        }

        return copy;
    }

    public static String convertToTimestamp(final long time) {
        final long second = (time / 1000) % 60;
        final long minute = (time / (1000 * 60)) % 60;
        final long hour = (time / (1000 * 60 * 60)) % 24;

        return String.format("%02d:%02d:%02d", hour, minute, second);
    }

    //Credit to Marimuthu Madasamy on stack overflow
    public static <T extends Comparable<T>> int findMinIndex(final List<T> xs) {
        int minIndex;
        if (xs.isEmpty()) {
            minIndex = -1;
        } else {
            final ListIterator<T> itr = xs.listIterator();
            T min = itr.next(); // first element as the current minimum
            minIndex = itr.previousIndex();
            while (itr.hasNext()) {
                final T curr = itr.next();
                if (curr.compareTo(min) < 0) {
                    min = curr;
                    minIndex = itr.previousIndex();
                }
            }
        }
        return minIndex;
    }

}
