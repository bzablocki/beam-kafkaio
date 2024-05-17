/*
 * Copyright 2023 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.dataflow.dce.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

public interface MyRunOptions extends DataflowPipelineOptions {
    // enum PipelineToRun {
    //     PUBLISH_TO_SOLACE_DIRECT,
    //     PUBLISH_TO_SOLACE_PERSISTENT,
    //     READ_FROM_SOLACE
    // }
    //
    // @Description("Pipeline to run")
    // @Default.Enum("PUBLISH_TO_SOLACE_DIRECT")
    // PipelineToRun getPipelineToRun();
    //
    // void setPipelineToRun(PipelineToRun name);
}
