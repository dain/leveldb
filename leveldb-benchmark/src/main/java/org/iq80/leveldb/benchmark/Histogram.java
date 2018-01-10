/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.benchmark;

import com.google.common.base.Strings;

public class Histogram
{
    static final double[] K_BUCKET_LIMIT = {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 45,
            50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 250, 300, 350, 400, 450,
            500, 600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000,
            3500, 4000, 4500, 5000, 6000, 7000, 8000, 9000, 10000, 12000, 14000,
            16000, 18000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 60000,
            70000, 80000, 90000, 100000, 120000, 140000, 160000, 180000, 200000,
            250000, 300000, 350000, 400000, 450000, 500000, 600000, 700000, 800000,
            900000, 1000000, 1200000, 1400000, 1600000, 1800000, 2000000, 2500000,
            3000000, 3500000, 4000000, 4500000, 5000000, 6000000, 7000000, 8000000,
            9000000, 10000000, 12000000, 14000000, 16000000, 18000000, 20000000,
            25000000, 30000000, 35000000, 40000000, 45000000, 50000000, 60000000,
            70000000, 80000000, 90000000, 100000000, 120000000, 140000000, 160000000,
            180000000, 200000000, 250000000, 300000000, 350000000, 400000000,
            450000000, 500000000, 600000000, 700000000, 800000000, 900000000,
            1000000000, 1200000000, 1400000000, 1600000000, 1800000000, 2000000000,
            2500000000.0, 3000000000.0, 3500000000.0, 4000000000.0, 4500000000.0,
            5000000000.0, 6000000000.0, 7000000000.0, 8000000000.0, 9000000000.0,
            1e200,
    };
    private final int kNumBuckets = 154;
    private double min;
    private double max;
    private double num;
    private double sum;
    private double sumSquares;

    private double[] doubles = new double[kNumBuckets];

    public void clear()
    {
        min = K_BUCKET_LIMIT[kNumBuckets - 1];
        max = 0;
        num = 0;
        sum = 0;
        sumSquares = 0;
        for (int i = 0; i < kNumBuckets; i++) {
            doubles[i] = 0;
        }
    }

    public void add(double value)
    {
        // Linear search is fast enough for our usage in db_bench
        int b = 0;
        while (b < kNumBuckets - 1 && K_BUCKET_LIMIT[b] <= value) {
            b++;
        }
        doubles[b] += 1.0;
        if (min > value) {
            min = value;
        }
        if (max < value) {
            max = value;
        }
        num++;
        sum += value;
        sumSquares += (value * value);
    }

    public void merge(Histogram other)
    {
        if (other.min < min) {
            min = other.min;
        }
        if (other.max > max) {
            max = other.max;
        }
        num += other.num;
        sum += other.sum;
        sumSquares += other.sumSquares;
        for (int b = 0; b < kNumBuckets; b++) {
            doubles[b] += other.doubles[b];
        }
    }

    public double median()
    {
        return percentile(50.0);
    }

    public double percentile(double p)
    {
        double threshold = num * (p / 100.0);
        double sum = 0;
        for (int b = 0; b < kNumBuckets; b++) {
            sum += doubles[b];
            if (sum >= threshold) {
                // Scale linearly within this bucket
                double leftPoint = (b == 0) ? 0 : K_BUCKET_LIMIT[b - 1];
                double rightPoint = K_BUCKET_LIMIT[b];
                double leftSum = sum - doubles[b];
                double rightSum = sum;
                double pos = (threshold - leftSum) / (rightSum - leftSum);
                double r = leftPoint + (rightPoint - leftPoint) * pos;
                if (r < min) {
                    r = min;
                }
                if (r > max) {
                    r = max;
                }
                return r;
            }
        }
        return max;
    }

    public double average()
    {
        if (num == 0.0) {
            return 0;
        }
        return sum / num;
    }

    public double standardDeviation()
    {
        if (num == 0.0) {
            return 0;
        }
        double variance = (sumSquares * num - sum * sum) / (num * num);
        return Math.sqrt(variance);
    }

    public String toString()
    {
        StringBuilder r = new StringBuilder();
        r.append(String.format("Count: %.0f  Average: %.4f  StdDev: %.2f\n",
                num, average(), standardDeviation()));
        r.append(String.format("Min: %.4f  Median: %.4f  Max: %.4f\n",
                (num == 0.0 ? 0.0 : min), median(), max));
        r.append("------------------------------------------------------\n");
        r.append("left     right      count        %      cum %   \n");
        double mult = 100.0 / num;
        double sum = 0;
        for (int b = 0; b < kNumBuckets; b++) {
            if (doubles[b] <= 0.0) {
                continue;
            }
            sum += doubles[b];
            r.append(String.format("[ %7.0f, %7.0f ) %7.0f %7.3f%% %7.3f%% ",
                    ((b == 0) ? 0.0 : K_BUCKET_LIMIT[b - 1]),      // left
                    K_BUCKET_LIMIT[b],                           // right
                    doubles[b],                               // count
                    mult * doubles[b],                        // percentage
                    mult * sum));                               // cumulative percentage

            // Add hash marks based on percentage; 20 marks for 100%.
            int marks = (int) (20 * (doubles[b] / num) + 0.5);
            r.append(Strings.repeat("#", marks));
            r.append("\n");
        }
        return r.toString();
    }
}
