import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css']
})
export class LoginComponent implements OnInit {

  constructor() { }

  connectStatus: string;

  ipAdd:string;

  ngOnInit() {
    this.connectStatus = "Not Connected";
    this.ipAdd= "";
  }

  connectToHBase(ipAddress){
    this.connectStatus = "Connecting to "+ipAddress+"...";
  }


}
