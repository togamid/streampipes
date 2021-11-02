/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import { Component, Input, OnInit } from '@angular/core';
import { AdapterDescriptionUnion, SpQueryResult } from '../../../../core-model/gen/streampipes-model';
import { DatalakeRestService } from '../../../../platform-services/apis/datalake-rest.service';
import { DatalakeQueryParameterBuilder } from '../../../../core-services/datalake/DatalakeQueryParameterBuilder';
import { FieldConfig } from '../../../../data-explorer/models/dataview-dashboard.model';
import { EChartsOption } from 'echarts';


@Component({
  selector: 'sp-adapter-details',
  templateUrl: './adapter-details.component.html',
  styleUrls: ['./adapter-details.component.scss']
})
export class AdapterDetailsComponent implements OnInit {

  @Input()
  adapterDetails: AdapterDescriptionUnion;

  constructor(
    protected dataLakeRestService: DatalakeRestService) {
  }

  chartOption: EChartsOption = undefined;
  data: any[] = undefined;

  ngOnInit(): void {
    const oneHour = 60 * 60 * 1000;
    const now = new Date().getTime();


    const countField: FieldConfig = { runtimeName: 'count', aggregations: ['SUM'], selected: true, numeric: false };

    const queryParameters = DatalakeQueryParameterBuilder
      .create(now - oneHour, now)
      .withAggregation('m', 1)
      .withColumnFilter([countField], true)
      .build();

    this.dataLakeRestService.getData('internalmonitoradapter', queryParameters).subscribe((res: SpQueryResult) => {
      this.chartOption = this.transformData(res);
    });
  }

  transformData(data: SpQueryResult): EChartsOption {
    const chartOption: EChartsOption = {
      xAxis: {
        type: 'category',
        data: []
      },
      yAxis: {
        type: 'value'
      },
      series: [
        {
          data: [],
          type: 'bar'
        }
      ]
    };

    data.allDataSeries[0].rows.forEach(row => {
      chartOption.xAxis['data'].push(row[0]);
      chartOption.series[0].data.push(row[1]);
    });
    return chartOption;
  }

}
