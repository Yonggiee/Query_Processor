package qp.utils;

import java.io.*;
import java.util.*;

import qp.operators.Operator;
import qp.operators.OrderByComparator;
import qp.operators.OrderType;

public class Sort {
    private int numBuff;
    private Operator base;
    private ArrayList<File> sortedRuns = new ArrayList<>();
    private int batchsize;
    private int runNum = 0;
    private OrderByComparator comparator;

    public Sort(Operator base, int numBuff, List<OrderType> orderTypes, int batchsize) {
        this.base = base;
        this.numBuff = numBuff;
        this.comparator = new OrderByComparator(base.getSchema(), orderTypes);
        this.batchsize = batchsize;
    }

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

        // change to be able to do desc
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

    private ArrayList<Tuple> insertInSortedOrder(ArrayList<Tuple> tupleList, ArrayList<Integer> sortedOrder,
            int fromWhichSortedRun, Tuple toInsert) {
        if (tupleList.size() == 0) {
            tupleList.add(toInsert);
            sortedOrder.add(fromWhichSortedRun);
        } else {
            boolean inserted = false;
            for (int i = 0; i < tupleList.size(); i++) {
                if (comparator.compare(tupleList.get(i), toInsert) > 0) {
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
                    Arrays.sort(tuplesInRun, comparator);
                    try {
                        sortedRun = File.createTempFile(runNum + "-temp", null, new File("./"));
                        sortedRun.deleteOnExit();
                        fileWriter = new ObjectOutputStream(new FileOutputStream(sortedRun));
                        for (int j = 0; j < totalTuplesInBuffs; j++) {
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
            Arrays.sort(tuplesInRun, comparator);
            try {
                sortedRun = File.createTempFile(runNum + "-temp", null, new File("./"));
                sortedRun.deleteOnExit();
                fileWriter = new ObjectOutputStream(new FileOutputStream(sortedRun));
                for (int j = 0; j < added; j++) {
                    fileWriter.writeObject(tuplesInRun[j]);
                }
                fileWriter.writeObject(null);
                sortedRuns.add(sortedRun);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
