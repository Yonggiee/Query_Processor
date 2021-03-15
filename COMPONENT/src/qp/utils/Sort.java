package qp.utils;

import java.io.*;
import java.util.*;

import qp.operators.Operator;
import qp.operators.OrderByComparator;
import qp.operators.OrderType;

public class Sort {
    private int numBuff;
    private Operator base;
    private ArrayList<File> sortedRuns = new ArrayList<>();    // The sorted runs in the current run number
    private int batchsize;
    private int runNum = 0;                                    // The current run number
    private OrderByComparator comparator;                      // Comparator to know the ordering that the tuples should be sorted
    private boolean isDistinct = false;                        // Whether it is an distinct keyword or just a orderby/sort merge

    // Sort constructor for distinct and sort merge join
    public Sort(Operator base, int numBuff, int batchsize, boolean isDistinct) {
        this.base = base;
        this.numBuff = numBuff;
        this.batchsize = batchsize;
        this.comparator = new OrderByComparator(base.getSchema());
        this.isDistinct = isDistinct;
    }

    // Sort constructor for orderby so orderType list is required
    public Sort(Operator base, int numBuff, List<OrderType> orderTypes, int batchsize) {
        this.base = base;
        this.numBuff = numBuff;
        this.comparator = new OrderByComparator(base.getSchema(), orderTypes);
        this.batchsize = batchsize;
    }
    
    // Perform the sorting and output an ObjectInputStream
    public ObjectInputStream performSort() {
        generateSortedRuns();
        performMerge();
        try {
            return new ObjectInputStream(new FileInputStream(sortedRuns.get(0)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Constantly merges the sorted files with the available buffers 
     * Optimise the number of I/Os by reducing number of runs and using the maximum number of
     * files to merge with the given buffers.
     * Outputs temperary files for that run.
     **/
    private void performMerge() {
        int noOfSortedRuns = sortedRuns.size();
        int noOfMergePasses = (int) Math.ceil(Math.log(noOfSortedRuns) / Math.log(numBuff - 1));
        for (int i = 0; i < noOfMergePasses; i++) {
            noOfSortedRuns = sortedRuns.size();
            runNum += 1;
            ArrayList<File> newSortedRuns = new ArrayList<>();

            int idx = 0;
            while(idx < noOfSortedRuns) {
                int end = (idx + numBuff - 1) - 1 >= noOfSortedRuns ? noOfSortedRuns - 1 : (idx + numBuff - 1) - 1;
                File mergedSortedRun = mergeSortedRuns(idx, end);
                newSortedRuns.add(mergedSortedRun);
                idx = end + 1;
            }
            sortedRuns = newSortedRuns;
        }
    }

    /**
     * Merge files from the index start to end inclusive.
     * Simulate buffers for each of the files and populate buffers with tuples from the file.
     * Get the first tuple of each file and check which should be output first 
     * by using the comparator.
     * After that, add another tuple from the file that just been output.
     * Repeat until no more tuples from the files.
     **/
    private File mergeSortedRuns(int start, int end) {
        int numOfSortedRuns = end - start + 1;
        int numBuffersPerSortedRun = (numBuff - 1) / numOfSortedRuns;
        int numTuplesPerSortedRun = numBuffersPerSortedRun * batchsize;
        if (numOfSortedRuns == 1) {
            return sortedRuns.get(start);
        }

        ArrayList<ArrayList<Tuple>> sortedRunsForThisRound = new ArrayList<ArrayList<Tuple>>(numOfSortedRuns);
        for (int i = 0; i < numOfSortedRuns; i++) {
            sortedRunsForThisRound.add(new ArrayList<Tuple>());
        }
        Boolean[] trackToStopForEachSortedRun = new Boolean[numOfSortedRuns];
        ArrayList<ObjectInputStream> streams = new ArrayList<>();

        for (int i = start; i < numOfSortedRuns + start; i++) {
            File run = sortedRuns.get(i);
            ObjectInputStream ois = null;
            trackToStopForEachSortedRun[i - start] = false;
            try {
                ois = new ObjectInputStream(new FileInputStream(run));
                streams.add(ois);
                sortedRunsForThisRound.set(i - start,
                        fillBuffers(ois, i - start, numTuplesPerSortedRun, trackToStopForEachSortedRun));
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        Tuple[] tuplesInRun = new Tuple[batchsize];
        int added = 0;

        ArrayList<Tuple> topOfAllSortedRuns = new ArrayList<>();
        ArrayList<Integer> trackOrderOfFirsts = new ArrayList<>();
        for (int i = 0; i < numOfSortedRuns; i++) {
            topOfAllSortedRuns = insertInSortedOrder(topOfAllSortedRuns, trackOrderOfFirsts, i,
                    sortedRunsForThisRound.get(i).get(0));
            sortedRunsForThisRound.get(i).remove(0);
        }

        File sortedRun = null;
        ObjectOutputStream fileWriter = null;
        try {
            sortedRun = File.createTempFile(runNum + "-temp", null, new File("./"));
            sortedRun.deleteOnExit();
            fileWriter = new ObjectOutputStream(new FileOutputStream(sortedRun));
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            if (topOfAllSortedRuns.size() == 0) {
                break;
            }
            Tuple first = topOfAllSortedRuns.get(0);
            int whichSortedRun = trackOrderOfFirsts.get(0);
            topOfAllSortedRuns.remove(0);
            trackOrderOfFirsts.remove(0);

            if (added == batchsize) {
                try {
                    for (int j = 0; j < batchsize; j++) {
                        fileWriter.writeObject(tuplesInRun[j]);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                tuplesInRun = new Tuple[batchsize];
                added = 0;
            }
            tuplesInRun[added] = first;
            added += 1;
            if (sortedRunsForThisRound.get(whichSortedRun).size() == 0) {
                if (!trackToStopForEachSortedRun[whichSortedRun]) {
                    try {
                        sortedRunsForThisRound.set(whichSortedRun, fillBuffers(streams.get(whichSortedRun),
                                whichSortedRun, numTuplesPerSortedRun, trackToStopForEachSortedRun));
                    } catch (ClassNotFoundException | IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (sortedRunsForThisRound.get(whichSortedRun).size() > 0) {
                topOfAllSortedRuns = insertInSortedOrder(topOfAllSortedRuns, trackOrderOfFirsts, whichSortedRun,
                        sortedRunsForThisRound.get(whichSortedRun).get(0));
                sortedRunsForThisRound.get(whichSortedRun).remove(0);
            }
        }
        if (added > 0) {
            try {
                for (int j = 0; j < added; j++) {
                    fileWriter.writeObject(tuplesInRun[j]);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            fileWriter.writeObject(null);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sortedRun;
    }

    /**
     * Fill buffers with tuples from the file.
     **/
    private ArrayList<Tuple> fillBuffers(ObjectInputStream ois, int fileNo, int noToFill,
            Boolean[] trackToStopForEachSortedRun) throws FileNotFoundException, IOException, ClassNotFoundException {
        ArrayList<Tuple> filledBuffers = new ArrayList<>();
        for (int j = 0; j < noToFill; j++) {
            Object inStream = ois.readObject();
            if (inStream == null) {
                trackToStopForEachSortedRun[fileNo] = true;
                break;
            } else {
                filledBuffers.add((Tuple) inStream);
            }
        }
        return filledBuffers;
    }

    /**
     * Insert tuples into sorted order according to the comparator.
     * If this is DISTINCT, tuple will not be inserted if the two tuples are equal.
     **/
    private ArrayList<Tuple> insertInSortedOrder(ArrayList<Tuple> tupleList, ArrayList<Integer> sortedOrder,
            int fromWhichSortedRun, Tuple toInsert) {
        if (tupleList.size() == 0) {
            tupleList.add(toInsert);
            sortedOrder.add(fromWhichSortedRun);
        } else {
            boolean inserted = false;
            for (int i = 0; i < tupleList.size(); i++) {
                if (isDistinct && comparator.compare(tupleList.get(i), toInsert) == 0) {
                    inserted = true;
                    break;
                } else if (comparator.compare(tupleList.get(i), toInsert) > 0) {
                    tupleList.add(i, toInsert);
                    sortedOrder.add(i, fromWhichSortedRun);
                    inserted = true;
                    break;
                }
            }
            if (!inserted) {
                tupleList.add(toInsert);
                sortedOrder.add(fromWhichSortedRun);
            }
        }
        return tupleList;
    }

    /**
     * Uses (total buffers - 1) buffers to create the first sorted runs.
     * Buffers are populated with tuples and sorted accorded to the comparator.
     * If this is DISTINCT, the duplicate tuples will be removed.
     **/
    private void generateSortedRuns() {
        int totalTuplesInBuffs = batchsize * numBuff;
        Tuple[] tuplesInRun = new Tuple[totalTuplesInBuffs];
        int added = 0;
        Batch inbatch = base.next();

        File sortedRun = null;
        ObjectOutputStream fileWriter = null;
        while (inbatch != null) {
            for (int i = 0; i < inbatch.size(); i++) {
                Tuple tuple = inbatch.get(i);
                if (added == totalTuplesInBuffs) {
                    if (isDistinct) {
                        tuplesInRun = removeDuplicates(tuplesInRun);
                    } else {
                        Arrays.sort(tuplesInRun, comparator);
                    }
                    try {
                        sortedRun = File.createTempFile(runNum + "-temp", null, new File("./"));
                        sortedRun.deleteOnExit();
                        fileWriter = new ObjectOutputStream(new FileOutputStream(sortedRun));
                        for (int j = 0; j < tuplesInRun.length; j++) {
                            fileWriter.writeObject(tuplesInRun[j]);
                        }
                        fileWriter.writeObject(null);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }
                    sortedRuns.add(sortedRun);
                    tuplesInRun = new Tuple[totalTuplesInBuffs];
                    added = 0;
                }
                tuplesInRun[added] = tuple;
                added += 1;
            }
            inbatch = base.next();
        }

        if (added > 0) {
            tuplesInRun = Arrays.copyOfRange(tuplesInRun, 0, added);
            if (isDistinct) {
                tuplesInRun = removeDuplicates(tuplesInRun);
            } else {
                Arrays.sort(tuplesInRun, comparator);
            }
            try {
                sortedRun = File.createTempFile(runNum + "-temp", null, new File("./"));
                sortedRun.deleteOnExit();
                fileWriter = new ObjectOutputStream(new FileOutputStream(sortedRun));
                for (int j = 0; j < tuplesInRun.length; j++) {
                    fileWriter.writeObject(tuplesInRun[j]);
                }
                fileWriter.writeObject(null);
                sortedRuns.add(sortedRun);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Remove duplicates for DISTINCT
    private Tuple[] removeDuplicates(Tuple[] toRemove) {
        Set<Tuple> sortedAndMerged = new TreeSet<Tuple>(comparator);
        for (int i = 0; i < toRemove.length; i++) {
            sortedAndMerged.add(toRemove[i]);
        }

        Tuple[] output = new Tuple[sortedAndMerged.size()];
        sortedAndMerged.toArray(output);
        return output;
    }

}
