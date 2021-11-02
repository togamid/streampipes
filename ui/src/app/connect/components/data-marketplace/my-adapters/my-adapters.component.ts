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

import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { AdapterDescriptionUnion } from '../../../../core-model/gen/streampipes-model';

@Component({
  selector: 'sp-my-adapters',
  templateUrl: './my-adapters.component.html',
  styleUrls: ['./my-adapters.component.scss']
})
export class MyAdaptersComponent implements OnInit {

  @Input()
  existingAdapters: AdapterDescriptionUnion[];

  @Output()
  updateAdapterEmitter: EventEmitter<void> = new EventEmitter<void>();

  @Output()
  createTemplateEmitter: EventEmitter<AdapterDescriptionUnion> = new EventEmitter<AdapterDescriptionUnion>();

  showAllAdapters: boolean;
  adapterDetails: AdapterDescriptionUnion;

  constructor() {
  }

  ngOnInit(): void {
    this.showAllAdapters = true;
  }

  updateDescriptionsAndRunningAdatpers() {
    this.updateAdapterEmitter.emit();
  }

  createTemplate(adapter: AdapterDescriptionUnion): void {
    this.createTemplateEmitter.emit(adapter);
  }

  showAdapterDetails(adapter: AdapterDescriptionUnion): void {
    this.showAllAdapters = false;
    this.adapterDetails = adapter;
  }
}
