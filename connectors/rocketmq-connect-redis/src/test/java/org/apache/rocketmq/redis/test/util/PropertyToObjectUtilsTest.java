/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.redis.test.util;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.redis.util.PropertyToObjectUtils;
import org.junit.Assert;
import org.junit.Test;

public class PropertyToObjectUtilsTest {

    @Test
    public void test(){
        User user = new User();
        user.setName("LLL");
        user.setAge(100);
        user.setScore(365);
        user.setAverage(66.66);
        user.setMath(78L);
        user.setHigh(false);

        KeyValue pair = new DefaultKeyValue();
        pair.put("name", "LLL");
        pair.put("age", "100");
        pair.put("score", "365");
        pair.put("average", "66.66");
        pair.put("math", "78");
        pair.put("high", "false");

        User user1 = new User();
        Exception ex = null;
        try {
            PropertyToObjectUtils.properties2Object(pair, user1);
        } catch (InvocationTargetException e) {
            ex = e;
        } catch (IllegalAccessException e) {
            ex = e;
        }
        Assert.assertEquals(user, user1);
        Assert.assertNull(ex);
    }


    public class User{
        private String name;
        private Integer age;
        private int score;
        private Long math;
        private Double average;
        private boolean high;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public int getScore() {
            return score;
        }

        public void setScore(int score) {
            this.score = score;
        }

        public Long getMath() {
            return math;
        }

        public void setMath(Long math) {
            this.math = math;
        }

        public Double getAverage() {
            return average;
        }

        public void setAverage(Double average) {
            this.average = average;
        }

        public void setHigh(boolean high) {
            this.high = high;
        }

        public boolean isHigh() {
            return high;
        }

        @Override
        public boolean equals(Object obj) {
            if(this == obj){
                return true;
            }
            if(obj == null || getClass() != obj.getClass()){
                return false;
            }
            User other = (User)obj;
            return Objects.equals(this.getName(), other.getName()) &&
                Objects.equals(this.getAge(), other.getAge()) &&
                Objects.equals(this.getAverage(), other.getAverage()) &&
                Objects.equals(this.getMath(), other.getMath()) &&
                Objects.equals(this.getScore(), other.getScore());
        }
    }
}
