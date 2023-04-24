const mongoose = require('mongoose');
const { Lop, SinhVien, Khoa, KetQua, MonHoc } = require('../models');
const { query } = require('express');

/*
    1.Liệt kê danh sách các lớp của khoa, thông tin cần Malop, TenLop, MaKhoa 
    SELECT * FROM Lop
*/
const cau1 = async () => {
  const results = await Lop.find()
  return results;
};

/*
    2.Lập danh sách sinh viên gồm: MaSV, HoTen, HocBong 
    SELECT MaSV, Hoten, HocBong FROM SinhVien
*/
const cau2 = async () => {
  const results = await SinhVien.aggregate([
    {
      $project: { _id: 1, hoTen: 1, hocBong: 1},
    },
  ]);
  return results;
};

/*
    3.Lập danh sách sinh viên có học bổng. Danh sách cần MaSV, Nu, HocBong
    SELECT MaSV, Nu, HocBong FROM SinhVien WHERE HocBong>0
*/
const cau3 = async () => {
  const results = await SinhVien.aggregate([
    {
      $lookup: {
        from: Lop.collection.name,
        localField: 'maLop',
        foreignField: 'id',
        as: 'lop',
      },
    },
    {
      $replaceRoot: {
        newRoot: { $mergeObjects: [{ $arrayElemAt: ['$lop', 0] }, '$$ROOT'] },
      },
    },
    {

      $match: {
        hocBong: { $ne: null, $gt: 0 }
      },
    },
    {
      $project: { _id: 1, hoTen: 1, hocBong: 1, tenLop: 1 },
    },
  ]);
  return results;
};

/*
    4.Lập danh sách sinh viên nữ. Danh sách cần các thuộc tính của 
    quan hệ sinhvien 
    SELECT * FROM SinhVien WHERE Nu =Yes
*/
const cau4 = async () => {
  const results = await SinhVien.find().where('nu').equals('Yes');
  return results;
};

/*
    5.Lập danh sách sinh viên có họ ‘Trần’ 
    SELECT * FROM SinhVien WHERE HoTen Like ‘Trần *’
*/
const cau5 = async () => {
  const results = await SinhVien.find().where({ hoTen: /^Trần/ });
  return results;
};

/*
    6.Lập danh sách sinh viên nữ có học bổng 
    SELECT * FROM SinhVien WHERE Nu=Yes AND HocBong>0
*/
const cau6 = async () => {
  const results = await SinhVien.aggregate([
    {
      $match: {
        $and:[
          {hocBong: { $ne: null, $gt: 0 }},
          {nu:'Yes'}
        ]
      }    
    },
    {
      $project: { _id: 1, hoTen: 1},
    },
  ]);
  return results;
};
/*
    7.Lập danh sách sinh viên nữ hoặc danh sách sinh viên có học bổng 
    SELECT * FROM SinhVien WHERE Nu=Yes OR HocBong>0
*/
const cau7 = async () => {
  const results = await SinhVien.find({
    $or: [{ nu: 'Yes' }, { hocBong: { $ne: null, $gt: 0} }],
  });
  return results;
};

/*
    8.Lập danh sách sinh viên có năm sinh từ 1978 đến 1985. 
    Danh sách cần các thuộc tính của quan hệ SinhVien 
    SELECT * FROM SinhVien WHERE YEAR(NgaySinh) BETWEEN 1978 AND 1985
*/
const cau8 = async () => {
  const results = await SinhVien.find({
    ngaySinh: {
      //từ
      $gte: new Date(1978, 1, 1),
      // đến
      $lt: new Date(1986, 1, 1),
    },
  });
  return results;
};

/*
    9.Liệt kê danh sách sinh viên được sắp xếp tăng dần theo MaSV hoặc id
    SELECT * FROM SinhVien ORDER BY MaSV
*/
const cau9 = async () => {
  // const results = await SinhVien.find().sort('id');
  const results = await SinhVien.find().sort('hoTen');
  return results;
};

/*
    10.Liệt kê danh sách sinh viên được sắp xếp giảm dần theo HocBong 
    SELECT * FROM SinhVien ORDER BY HocBong DESC 
*/
const cau10 = async () => {
  const results = await SinhVien.find().sort({hocBong: -1});
  return results;
};

/*
  Câu 11: Lập danh sách sinh viên có điểm thi môn Lập trình JavaScript 1 >= 8
  SELECT SinhVien.MaSV, HoTen, Nu, NgaySinh, DiemThi
  FROM SinhVien INNER JOIN KetQua ON SinhVien.MaSV = KetQua.MaSV
  WHERE MaMH = ‘CSDL’ AND DiemThi>=8  
*/
const cau11 = async () => {
  const results = await KetQua.aggregate([
    {
      $lookup: {
        from: 'monhocs',
        localField: 'maMH',
        foreignField: '_id',
        as: 'MonHoc',
      },
    },
    {
      $replaceRoot: {
        newRoot: { $mergeObjects: [{ $arrayElemAt: ['$MonHoc', 0] }, '$$ROOT'] },
      },
    },
    {
      $project: {
        MonHoc: 0
      }
    },
    {
      $match: {
        tenMH:'Lập trình JavaScript 1'
      },
    },
    {
      $match: {
        diemThi: { $gte: 8 }
      }
    },
    {
      $lookup: {
        from: 'sinhviens',
        localField: 'maSV',
        foreignField: '_id',
        as: 'sv',
      },
    },
    {
      $replaceRoot: {
        newRoot: { $mergeObjects: [{ $arrayElemAt: ['$sv', 0] }, '$$ROOT'] },
      },
    },
    {
      $project: {
        sv: 0
      }
    },
    {
      $project: { 
        _id: 1, hoTen: 1, nu:1, ngaySinh:1,
tinh: 1,diemThi: 1
      },
    },
  ]);
  return results;
};

/*
    12.Lập danh sách sinh viên có học bổng của khoa Cơ khí. 
    Thông tin cần: MaSV, HoTen, HocBong,TenLop 

    SELECT MaSV, HoTen, HocBong, TenLop 
    FROM Lop INNER JOIN SinhVien ON Lop.MaLop=SinhVien.MaLop 
    WHERE HocBong>0 AND MaKhoa =’CNTT’
*/
const cau12 = async () => {
  const results = await SinhVien.aggregate([
    {
      $lookup: {
        from: Lop.collection.name,
        localField: 'maLop',
        foreignField: '_id',
        as: 'lop',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$lop', 0] }, '$$ROOT'] } },
    },
    { $project: { lop: 0 } },
    {
      $lookup: {
        from: Khoa.collection.name,
        localField: 'maKhoa',
        foreignField: '_id',
        as: 'khoa',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$khoa', 0] }, '$$ROOT'] } },
    },
    { $project: { khoa: 0 } },
    {
      $match: {
        // $and: [{ hocBong: { $gt: 0 } }, { maKhoa: mongoose.Types.ObjectId('6445e0c47baec0365c96a156') }],
        $and: [{ hocBong: { $gt: 0 } }, { tenKhoa: 'Cơ Khí' }],
      },
    },
    {
      $project: { _id: 1, hoTen: 1, hocBong: 1, tenLop: 1 },
    },
  ]);
  return results;
};

/*
    13.Lập danh sách sinh viên có học bổng của khoa Cơ khí. 
    Thông tin cần: MaSV, HoTen, HocBong,TenLop, TenKhoa 
    SELECT MaSV, HoTen, HocBong, TenLop,TenKhoa 
    FROM (
        (Lop INNER JOIN SinhVien ON Lop.MaLop=SinhVien.MaLop) 
        INNER JOIN Khoa ON Khoa.MaKhoa=Lop.MaKhoa
        ) 
    WHERE HocBong>0 AND Khoa.MaKhoa =’CNTT’
*/
const cau13 = async () => {
  const results = await SinhVien.aggregate([
    {
      $lookup: {
        from: Lop.collection.name,
        localField: 'maLop',
        foreignField: '_id',
        as: 'lop',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$lop', 0] }, '$$ROOT'] } },
    },
    { $project: { lop: 0 } },
    {
      $lookup: {
        from: Khoa.collection.name,
        localField: 'maKhoa',
        foreignField: '_id',
        as: 'khoa',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$khoa', 0] }, '$$ROOT'] } },
    },
    { $project: { khoa: 0 } },
    {
      $match: {
        // $and: [{ hocBong: { $gt: 0 } }, { maKhoa: mongoose.Types.ObjectId('6445e0c47baec0365c96a156') }],
        $and: [{ hocBong: { $gt: 0 } }, { tenKhoa: 'Cơ Khí' }],
      },
    },
    {
      $project: { _id: 1, hoTen: 1, hocBong: 1, tenLop: 1, tenKhoa: 1 },
    },
  ]);
  return results;
};

/*
    14.Cho biết số sinh viên của mỗi lớp 
    SELECT Lop.MaLop, TenLop, Count(MaSV) as SLsinhvien 
    FROM Lop INNER JOIN SinhVien ON Lop.MaLop = SinhVien.MaLop 
    GROUP BY Lop.MaLop, TenLop
*/
const cau14 = async () => {
  const results = await SinhVien.aggregate([
    {
      $group: { _id: '$maLop', SLSinhVien: { $sum: 1 } },
    },
    {
      $lookup: {
        from: Lop.collection.name,
        localField: '_id',
        foreignField: '_id',
        as: 'lop',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$lop', 0] }, '$$ROOT'] } },
    },
    {
      $project: { _id: 1, tenLop: 1, SLSinhVien: 1 },
    },
  ]);
  return results;
};

/*
    15.Cho biết số lượng sinh viên của mỗi khoa. 
    SELECT Khoa.MaKhoa, TenKhoa, Count(MaSV) as SLsinhvien 
    FROM (
        (Khoa INNER JOIN Lop ON Khoa.Makhoa = Lop.MaKhoa)
        INNER JOIN SinhVien ON Lop.MaLop = SinhVien.MaLop
        )
    GROUP BY Khoa.MaKhoa, TenKhoa
*/
const cau15 = async () => {
  const results = await SinhVien.aggregate([
    {
      $lookup: {
        from: Lop.collection.name,
        localField: 'maLop',
        foreignField: '_id',
        as: 'lop',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$lop', 0] }, '$$ROOT'] } },
    },
    {
      $lookup: {
        from: Khoa.collection.name,
        localField: 'maKhoa',
        foreignField: '_id',
        as: 'khoa',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$khoa', 0] }, '$$ROOT'] } },
    },
    {
      $group: { _id: '$maKhoa', tenKhoa: { $first: '$tenKhoa' }, SLSinhVien: { $sum: 1 } },
    },
  ]);
  return results;
};

/*
    16.Cho biết số lượng sinh viên nữ của mỗi khoa. 
    SELECT Khoa.MaKhoa, TenKhoa, Count(MaSV) as SLsinhvien 
    FROM (
        (SinhVien INNER JOIN Lop ON Lop.MaLop = SinhVien.MaLop) 
        INNER JOIN khoa ON KHOA.makhoa = Lop.makhoa
        ) 
    WHERE Nu=Yes 
    GROUP BY Khoa.MaKhoa, TenKhoa
*/
const cau16 = async () => {
  const results = await SinhVien.aggregate([
    {
      $match: { nu: 'Yes' },
    },
    {
      $lookup: {
        from: Lop.collection.name,
        localField: 'maLop',
        foreignField: '_id',
        as: 'lop',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$lop', 0] }, '$$ROOT'] } },
    },
    {
      $lookup: {
        from: Khoa.collection.name,
        localField: 'maKhoa',
        foreignField: '_id',
        as: 'khoa',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$khoa', 0] }, '$$ROOT'] } },
    },
    {
      $group: { _id: '$maKhoa', tenKhoa: { $first: '$tenKhoa' }, SLSinhVien: { $sum: 1 }  },
    },
  ]);
  return results;
};

/*
    17.Cho biết tổng tiền học bổng của mỗi lớp 
    SELECT Lop.MaLop, TenLop, Sum(HocBong) as TongHB 
    FROM (Lop INNER JOIN SinhVien ON Lop.MaLop = SinhVien.MaLop) 
    GROUP BY Lop.MaLop, TenLop
*/
const cau17 = async () => {
  const results = await SinhVien.aggregate([
    {
      $lookup: {
        from: Lop.collection.name,
        localField: 'maLop',
        foreignField: '_id',
        as: 'lop',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$lop', 0] }, '$$ROOT'] } },
    },
    {
      $group: { _id: '$maLop', tenLop: { $first: '$tenLop' }, tongHB: { $sum: '$hocBong' } },
    },
  ]);
  return results;
};

/*
    18.Cho biết tổng số tiền học bổng của mỗi khoa 
    SELECT Khoa.MaKhoa, TenKhoa, Sum(HocBong) as TongHB 
    FROM (
        (Khoa INNER JOIN Lop ON Khoa.Makhoa = Lop.MaKhoa)
        INNER JOIN SinhVien ON Lop.MaLop = SinhVien.MaLop
        ) 
    GROUP BY Khoa.MaKhoa, TenKhoa
*/
const cau18 = async () => {
  const results = await SinhVien.aggregate([
    {
      $lookup: {
        from: Lop.collection.name,
        localField: 'maLop',
        foreignField: '_id',
        as: 'lop',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$lop', 0] }, '$$ROOT'] } },
    },
    {
      $lookup: {
        from: Khoa.collection.name,
        localField: 'maKhoa',
        foreignField: '_id',
        as: 'khoa',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$khoa', 0] }, '$$ROOT'] } },
    },
    {
      $group: { _id: '$maKhoa', tenKhoa: { $first: '$tenKhoa' }, tongHB: { $sum: '$hocBong' } },
    },
  ]);
  return results;
};

/*
    19.Lập danh sánh những khoa có nhiều hơn 2 sinh viên. 
    Danh sách cần: MaKhoa, TenKhoa, Soluong 
    SELECT Khoa.MaKhoa, TenKhoa, Count(MaSV) as SLsinhvien 
    FROM (
        (Khoa INNER JOIN Lop ON Khoa.Makhoa = Lop.MaKhoa)
        INNER JOIN SinhVien ON Lop.MaLop = SinhVien.MaLop) 
    GROUP BY Khoa.MaKhoa, TenKhoa HAVING Count(MaSV) >2
*/
const cau19 = async () => {
  const results = await SinhVien.aggregate([
    {
      $lookup: {
        from: Lop.collection.name,
        localField: 'maLop',
        foreignField: '_id',
        as: 'lop',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$lop', 0] }, '$$ROOT'] } },
    },
    {
      $lookup: {
        from: Khoa.collection.name,
        localField: 'maKhoa',
        foreignField: '_id',
        as: 'khoa',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$khoa', 0] }, '$$ROOT'] } },
    },
    {
      $group: { _id: '$maKhoa', tenKhoa: { $first: '$tenKhoa' }, SLSinhVien: { $sum: 1 } },
    },
    {
      $match: {
        SLSinhVien: { $gt: 1 },
      },
    },
  ]);
  return results;
};

/*
    20.Lập danh sánh những khoa có nhiều hơn 2 sinh viên nữ. 
    Danh sách cần: MaKhoa, TenKhoa, Soluong 
    SELECT Khoa.MaKhoa, TenKhoa, Count(MaSV) as SLsinhvien 
    FROM (
        (Khoa INNER JOIN Lop ON Khoa.Makhoa = Lop.MaKhoa)
        INNER JOIN SinhVien ON Lop.MaLop = SinhVien.MaLop
        ) 
    WHERE Nu=Yes 
    GROUP BY Khoa.MaKhoa, TenKhoa HAVING Count(MaSV)>=2
*/
const cau20 = async () => {
  const results = await SinhVien.aggregate([
    {
      $match: { nu: 'Yes' },
    },
    {
      $lookup: {
        from: Lop.collection.name,
        localField: 'maLop',
        foreignField: '_id',
        as: 'lop',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$lop', 0] }, '$$ROOT'] } },
    },
    {
      $lookup: {
        from: Khoa.collection.name,
        localField: 'maKhoa',
        foreignField: '_id',
        as: 'khoa',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$khoa', 0] }, '$$ROOT'] } },
    },
    {
      $group: { _id: '$maKhoa', tenKhoa: { $first: '$tenKhoa' }, SLSinhVien: { $sum: 1 } },
    },
    {
      $match: {
        SLSinhVien: { $gte: 2 },
      },
    },
  ]);
  return results;
};

/*
  21.Lập danh sách những khoa có tổng tiền học bổng >=10.000.000.
  ta có HocBong = 10 <=> HocBong = 10.000.000 
  SELECT Khoa.MaKhoa, TenKhoa, Sum(HocBong) as TongHB 
  FROM (
    (Khoa INNER JOIN Lop ON Khoa.Makhoa = Lop.MaKhoa)
    INNER JOIN SinhVien ON Lop.MaLop = SinhVien.MaLop) 
  GROUP BY Khoa.MaKhoa, TenKhoa HAVING Sum(HocBong)>= 10
*/
const cau21 = async () => {
  const results = await SinhVien.aggregate([
    //FROM
    {
      $match: { nu: 'Yes' },
    },
    {
      $lookup: {
        from: Lop.collection.name,
        localField: 'maLop',
        foreignField: '_id',
        as: 'lop',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$lop', 0] }, '$$ROOT'] } },
    },
    {
      $lookup: {
        from: Khoa.collection.name,
        localField: 'maKhoa',
        foreignField: '_id',
        as: 'khoa',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$khoa', 0] }, '$$ROOT'] } },
    },
    {
      $group: { _id: '$maLop', tenKhoa: { $first: '$tenKhoa' }, tongHB: { $sum: '$hocBong' } },
    },

    {
      $match: { tongHB: { $gte: 10 } },
    },
  ]);
  return results;
};

/*
    22.Lập danh sách sinh viên có học bổng cao nhất 
    SELECT SinhVien.* FROM SinhVien 
    WHERE HocBong>= ALL(SELECT HocBong From Sinhvien)
*/
const cau22 = async () => {
  const results = await SinhVien.find().sort({ hocBong: -1 }).limit(1);
  return results;
};

/*
    23.Lập danh sách sinh viên có điểm thi môn CSDL cao nhất 
    SELECT SinhVien.MaSV, HoTen, DiemThi 
    FROM SinhVien INNER JOIN KetQua ON SinhVien.MaSV = KetQua.MaSV 
    WHERE KetQua.MaMH= ‘CSDL’ AND DiemThi>= ALL(SELECT DiemThi 
    FROM KetQua WHERE MaMH = ‘CSDL’)
*/
const cau23 = async () => {
  const results = await SinhVien.aggregate([
    {
      $lookup: {
        from: KetQua.collection.name,
        localField: '_id',
        foreignField: 'maSV',
        as: 'ketQua',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$ketQua', 0] }, '$$ROOT'] } },
    },
    {
      $project: { ketQua: 0 },
    },
    {
      $match: { maMH: mongoose.Types.ObjectId('6445e0e67baec0365c96a15c') },
    },
    {
      $sort: { diemThi: -1 },
    },
    {
      $limit: 1,
    },
    
  ]);
  return results;
};

/*
    24.Lập danh sách những sinh viên không có điểm thi môn CSDL. 
    SELECT SinhVien.MaSV, HoTen, DiemThi,MaMH 
    FROM SinhVien INNER JOIN KetQua ON SinhVien.MaSV = KetQua.MaSV 
    WHERE SinhVien.MaSV NOT In (Select MaSV From KetQua Where MaMH=’CSDL’)
*/
const cau24 = async () => {
  const ketQuaResult = await KetQua.find({ 
    maMH: mongoose.Types.ObjectId('6445e0df7baec0365c96a15a') 
  }).select('maSV');
  const results = await SinhVien.find({ _id: { $nin: ketQuaResult.map((x) => x.maSV) } });
  return results;
};

/*
    25.Cho biết những khoa nào có nhiều sinh viên nhất 
    SELECT Khoa.MaKhoa, TenKhoa, Count([MaSV]) AS SoLuongSV 
    FROM (
        (Khoa INNER JOIN Lop ON Khoa.MaKhoa = Lop.MaKhoa) 
        INNER JOIN SinhVien ON Lop.MaLop = SinhVien.MaLop 
    GROUP BY Khoa.MaKhoa, Khoa.TenKhoa HaVing Count(MaSV)>=All(Select Count(MaSV) 
    From (
        (SinhVien Inner Join Lop On Lop.Malop=SinhVien.Malop)
        Inner Join Khoa On Khoa.MaKhoa = Lop.MaKhoa )
    Group By Khoa.Makhoa)
*/
const cau25 = async () => {
  const results = await SinhVien.aggregate([
    {
      $lookup: {
        from: Lop.collection.name,
        localField: 'maLop',
        foreignField: '_id',
        as: 'lop',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$lop', 0] }, '$$ROOT'] } },
    },
    {
      $lookup: {
        from: Khoa.collection.name,
        localField: 'maKhoa',
        foreignField: '_id',
        as: 'khoa',
      },
    },
    {
      $replaceRoot: { newRoot: { $mergeObjects: [{ $arrayElemAt: ['$khoa', 0] }, '$$ROOT'] } },
    },
    {
      $group: { _id: '$maKhoa', tenKhoa: { $first: '$tenKhoa' }, SoLuongSV: { $sum: 1 } },
    },
    {
      $sort: { SoLuongSV: -1 },
    },
    {
      $limit: 1,
    },
  ]);
  return results;
};

module.exports = {
  cau1,
  cau2,
  cau3,
  cau4,
  cau5,
  cau6,
  cau7,
  cau8,
  cau9,
  cau10,
  cau11,
  cau12,
  cau13,
  cau14,
  cau15,
  cau16,
  cau17,
  cau18,
  cau19,
  cau20,
  cau21,
  cau22,
  cau23,
  cau24,
  cau25,
};
