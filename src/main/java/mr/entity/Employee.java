package mr.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yachao on 17/10/8.
 */
public class Employee implements WritableComparable<Employee> {
    private String empno;
    private String ename;
    private String job;
    private String mgr;
    private String hiredate;
    private int sal;
    private int comm;
    private String deptno;
    private String dname;
    private String loc;
    private boolean valid = true;

    public void set(Employee emp) {
        this.valid = emp.isValid();
        this.empno = emp.getEmpno();
        this.ename = emp.getEname();
        this.job = emp.getJob();
        this.mgr = emp.getMgr();
        this.hiredate = emp.getHiredate();
        this.sal = emp.getSal();
        this.comm = emp.getComm();
        this.deptno = emp.getDeptno();
        this.dname = emp.getDname();
        this.loc = emp.getLoc();
    }

    @Override
    public int compareTo(Employee o) {
        int total = this.sal + this.comm;
        int o_total = o.getSal() + o.getComm();

        if (total >= o_total) {
            return -1;
        }
        return 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(empno);
        out.writeUTF(ename);
        out.writeUTF(job);
        out.writeUTF(mgr);
        out.writeUTF(hiredate);
        out.writeInt(sal);
        out.writeInt(comm);
        out.writeUTF(deptno);
        out.writeUTF(dname);
        out.writeUTF(loc);
        out.writeBoolean(valid);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.empno = in.readUTF();
        this.ename = in.readUTF();
        this.job = in.readUTF();
        this.mgr = in.readUTF();
        this.hiredate = in.readUTF();
        this.sal = in.readInt();
        this.comm = in.readInt();
        this.deptno = in.readUTF();
        this.dname = in.readUTF();
        this.loc = in.readUTF();
        this.valid = in.readBoolean();
    }

    public static Employee parse(String line) {
        Employee emp = new Employee();

        String[] infos = line.split(",");
        if (infos.length >= 8) {
            emp.setEmpno(infos[0]);
            emp.setEname(infos[1]);
            emp.setJob(infos[2]);
            emp.setMgr(infos[3]);
            emp.setHiredate(infos[4]);

            if ("".equals(infos[5]) || infos[5] == null) {
                emp.setSal(0);
            } else {
                try {
                    emp.setSal(new Integer(infos[5]));
                } catch (NumberFormatException ex) {
                    emp.setSal(0);
                }
            }

            if ("".equals(infos[6]) || infos[6] == null) {
                emp.setComm(0);
            } else {
                try {
                    emp.setComm(new Integer(infos[6]));
                } catch (NumberFormatException ex) {
                    emp.setComm(0);
                }
            }

            emp.setDeptno(infos[7]);
            if ("10".equals(emp.getDeptno())) {
                emp.setDname("ACCOUNTING");
                emp.setLoc("NEW YORK");
            } else if ("20".equals(emp.getDeptno())) {
                emp.setDname("RESEARCH");
                emp.setLoc("DALLAS");
            } else if ("30".equals(emp.getDeptno())) {
                emp.setDname("SALES");
                emp.setLoc("CHICAGO");
            } else if ("40".equals(emp.getDeptno())) {
                emp.setDname("OPERATIONS");
                emp.setLoc("BOSTON");
            } else {
                emp.setDname("other");
                emp.setLoc("other");
            }
            emp.setValid(true);
        } else {
            emp.setValid(false);
        }
        return emp;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("valid:" + this.valid);
        sb.append("\nempno:" + this.empno);
        sb.append("\nename:" + this.ename);
        sb.append("\njob:" + this.job);
        sb.append("\nmgr:" + this.mgr);
        sb.append("\nhiredate:" + this.hiredate);
        sb.append("\nsal:" + this.sal);
        sb.append("\ncomm:" + this.comm);
        sb.append("\ndeptno:" + this.deptno);
        sb.append("\ndname:" + this.dname);
        sb.append("\nloc:" + this.loc);
        return sb.toString();
    }

    public String getEmpno() {
        return empno;
    }

    public void setEmpno(String empno) {
        this.empno = empno;
    }

    public String getEname() {
        return ename;
    }

    public void setEname(String ename) {
        this.ename = ename;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getMgr() {
        return mgr;
    }

    public void setMgr(String mgr) {
        this.mgr = mgr;
    }

    public String getHiredate() {
        return hiredate;
    }

    public void setHiredate(String hiredate) {
        this.hiredate = hiredate;
    }

    public int getSal() {
        return sal;
    }

    public void setSal(int sal) {
        this.sal = sal;
    }

    public int getComm() {
        return comm;
    }

    public void setComm(int comm) {
        this.comm = comm;
    }

    public String getDeptno() {
        return deptno;
    }

    public void setDeptno(String deptno) {
        this.deptno = deptno;
    }

    public String getDname() {
        return dname;
    }

    public void setDname(String dname) {
        this.dname = dname;
    }

    public String getLoc() {
        return loc;
    }

    public void setLoc(String loc) {
        this.loc = loc;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public static void main(String[] args) {
        String line = "7698,BLAKE,MANAGER,7839,1981-05-01,2850,,30";
        System.out.println(line);
        Employee emp = Employee.parse(line);
        System.out.println(emp);
    }
}
