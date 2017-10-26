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
package org.iq80.leveldb.impl;

import java.util.Objects;

/**
 * Value holder for reference modification like in C++
 */
final class ValueHolder<V>
{
    private V value;

    public ValueHolder(V value)
    {
        this.value = value;
    }

    /**
     * Setter for property 'value'.
     *
     * @param value Value to set for property 'value'.
     */
    public void setValue(V value)
    {
        this.value = value;
    }

    /**
     * Getter for property 'value'.
     *
     * @return Value for property 'value'.
     */
    public V getValue()
    {
        return value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ValueHolder<?> that = (ValueHolder<?>) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    @Override
    public String toString()
    {
        return "ValueHolder{value=" + value + '}';
    }
}
