/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.codesplit;

import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.util.FileUtils;

import org.junit.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Tests for {@link JavaCodeSplitter}. */
public class JavaCodeSplitterTest {

    @Test
    public void testSplitJavaCode() {
        runTest("TestSplitJavaCode", 100, 3);
    }

    @Test
    public void testNotSplitJavaCode() {
        runTest("TestNotSplitJavaCode", 4000, 10000);
    }

    @Test
    public void testInvalidJavaCode() {
        try {
            JavaCodeSplitter.split("public class InvalidClass { return 1; }", 4000, 10000);
        } catch (Exception e) {
            assertThat(e)
                    .satisfies(
                            matching(
                                    FlinkMatchers.containsMessage(
                                            "JavaCodeSplitter failed. This is a bug. Please file an issue.")));
        }
    }

    @Test
    public void testNullCode() {
        assertThatThrownBy(() -> JavaCodeSplitter.split(null, 4000, 10000))
                .getCause()
                .hasMessage("code cannot be empty");
    }

    @Test
    public void testEmptyCode() {
        assertThatThrownBy(() -> JavaCodeSplitter.split("", 4000, 10000))
                .getCause()
                .hasMessage("code cannot be empty");
    }

    @Test
    public void testWrongMaxMethodLength() {
        assertThatThrownBy(
                        () ->
                                JavaCodeSplitter.split(
                                        "public interface DummyInterface {}", 0, 10000))
                .getCause()
                .hasMessage("maxMethodLength must be greater than 0");
    }

    @Test
    public void testWrongMaxClassMemberCount() {
        assertThatThrownBy(
                        () -> JavaCodeSplitter.split("public interface DummyInterface {}", 10, 0))
                .getCause()
                .hasMessage("maxClassMemberCount must be greater than 0");
    }

    private void runTest(String filename, int maxLength, int maxMembers) {
        try {
            String code =
                    FileUtils.readFileUtf8(
                            new File(
                                    JavaCodeSplitterTest.class
                                            .getClassLoader()
                                            .getResource("splitter/code/" + filename + ".java")
                                            .toURI()));
            String expected =
                    FileUtils.readFileUtf8(
                            new File(
                                    JavaCodeSplitterTest.class
                                            .getClassLoader()
                                            .getResource("splitter/expected/" + filename + ".java")
                                            .toURI()));
            assertThat(JavaCodeSplitter.split(code, maxLength, maxMembers)).isEqualTo(expected);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // we reset the counter to ensure the variable names after rewrite are as expected
            CodeSplitUtil.getCounter().set(0L);
        }
    }
}
